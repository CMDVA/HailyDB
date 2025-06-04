import os
import logging
from flask import Flask, jsonify, request, render_template, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import atexit

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)

# Create the app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Configure the database
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL", "postgresql://localhost/nws_alerts")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}

# Initialize the app with the extension
db.init_app(app)

# Add custom Jinja2 filters
@app.template_filter('number_format')
def number_format(value):
    """Format numbers with commas for thousands"""
    try:
        return "{:,}".format(int(value))
    except (ValueError, TypeError):
        return value

# Import other modules after app initialization
from models import Alert, SPCReport, SPCIngestionLog, SchedulerLog
from ingest import IngestService
from enrich import EnrichmentService
from spc_ingest import SPCIngestService
from spc_matcher import SPCMatchingService
from spc_verification import SPCVerificationService
from scheduler_service import SchedulerService
from config import Config
import atexit

# Global services
ingest_service = None
enrich_service = None
spc_ingest_service = None
spc_matching_service = None
scheduler_service = None
scheduler = None
autonomous_scheduler = None

with app.app_context():
    # Import models to ensure tables are created
    import models
    db.create_all()
    
    # Initialize services
    ingest_service = IngestService(db)
    enrich_service = EnrichmentService(db)
    spc_ingest_service = SPCIngestService(db.session)
    spc_matching_service = SPCMatchingService(db.session)
    scheduler_service = SchedulerService(db)
    
    # Initialize autonomous scheduler
    from autonomous_scheduler import AutonomousScheduler
    autonomous_scheduler = AutonomousScheduler(db)

# API Routes
@app.route('/alerts')
def get_alerts():
    """Get recent alerts with optional filtering"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    severity = request.args.get('severity')
    event = request.args.get('event')
    state = request.args.get('state')
    county = request.args.get('county')
    area = request.args.get('area')
    effective_date = request.args.get('effective_date')
    active_only = request.args.get('active_only', 'false').lower() == 'true'
    
    query = Alert.query.order_by(Alert.effective.desc())
    
    if severity:
        query = query.filter(Alert.severity == severity)
    if event:
        query = query.filter(Alert.event.ilike(f'%{event}%'))
    if state:
        query = query.filter(Alert.area_desc.ilike(f'%{state}%'))
    if county:
        query = query.filter(Alert.area_desc.ilike(f'%{county}%'))
    if area:
        query = query.filter(Alert.area_desc.ilike(f'%{area}%'))
    if effective_date:
        from datetime import datetime
        try:
            filter_date = datetime.strptime(effective_date, '%Y-%m-%d').date()
            query = query.filter(db.func.date(Alert.effective) == filter_date)
        except ValueError:
            pass  # Invalid date format, ignore filter
    if active_only:
        from datetime import datetime
        now = datetime.utcnow()
        query = query.filter(
            Alert.effective <= now,
            Alert.expires > now
        )
    
    alerts = query.paginate(
        page=page,
        per_page=min(per_page, 100),
        error_out=False
    )
    
    if request.args.get('format') == 'json':
        return jsonify({
            'alerts': [{
                'id': alert.id,
                'event': alert.event,
                'severity': alert.severity,
                'area_desc': alert.area_desc,
                'effective': alert.effective.isoformat() if alert.effective else None,
                'expires': alert.expires.isoformat() if alert.expires else None,
                'ai_summary': alert.ai_summary,
                'ai_tags': alert.ai_tags
            } for alert in alerts.items],
            'pagination': {
                'page': alerts.page,
                'pages': alerts.pages,
                'per_page': alerts.per_page,
                'total': alerts.total
            }
        })
    
    # Get actual alert types for dropdown
    alert_types = db.session.query(Alert.event).distinct().order_by(Alert.event).all()
    alert_types_list = [row[0] for row in alert_types if row[0]]
    
    return render_template('alerts.html', alerts=alerts, alert_types=alert_types_list)

@app.route('/alerts/<alert_id>')
def get_alert(alert_id):
    """Get single enriched alert"""
    alert = Alert.query.get_or_404(alert_id)
    
    if request.args.get('format') == 'json':
        return jsonify({
            'id': alert.id,
            'event': alert.event,
            'severity': alert.severity,
            'area_desc': alert.area_desc,
            'effective': alert.effective.isoformat() if alert.effective else None,
            'expires': alert.expires.isoformat() if alert.expires else None,
            'sent': alert.sent.isoformat() if alert.sent else None,
            'geometry': alert.geometry,
            'properties': alert.properties,
            'raw': alert.raw,
            'ai_summary': alert.ai_summary,
            'ai_tags': alert.ai_tags,
            'spc_verified': alert.spc_verified,
            'spc_reports': alert.spc_reports,
            'ingested_at': alert.ingested_at.isoformat()
        })
    
    return render_template('alert_detail.html', alert=alert)

@app.route('/alerts/summary')
def get_alerts_summary():
    """Get recent alert summaries with AI-generated content and verification summaries"""
    # Get alerts with either AI summaries or SPC verification summaries
    alerts = Alert.query.filter(
        (Alert.ai_summary.isnot(None)) | (Alert.spc_ai_summary.isnot(None))
    ).order_by(Alert.ingested_at.desc()).limit(20).all()
    
    summaries = []
    for alert in alerts:
        summary_data = {
            'id': alert.id,
            'event': alert.event,
            'severity': alert.severity,
            'area_desc': alert.area_desc,
            'effective': alert.effective.isoformat() if alert.effective else None,
            'expires': alert.expires.isoformat() if alert.expires else None,
            'ai_summary': alert.ai_summary,
            'ai_tags': alert.ai_tags,
            'spc_verified': alert.spc_verified,
            'spc_verification_summary': alert.spc_ai_summary,
            'spc_confidence_score': alert.spc_confidence_score,
            'spc_report_count': alert.spc_report_count
        }
        
        # Add verification status for quick filtering
        if alert.spc_verified and alert.spc_ai_summary:
            summary_data['verification_status'] = 'verified_with_ai_summary'
        elif alert.spc_verified:
            summary_data['verification_status'] = 'verified'
        elif alert.ai_summary:
            summary_data['verification_status'] = 'ai_summary_only'
        else:
            summary_data['verification_status'] = 'basic'
            
        summaries.append(summary_data)
    
    if request.args.get('format') == 'json':
        return jsonify({
            'summaries': summaries,
            'total_count': len(summaries),
            'verified_count': len([s for s in summaries if s['spc_verified']]),
            'ai_summary_count': len([s for s in summaries if s['spc_verification_summary']])
        })
    
    return render_template('summaries.html', summaries=summaries)

@app.route('/api/alerts/by-state/<state>')
def get_alerts_by_state(state):
    """Get alerts for a specific state"""
    query = Alert.query.filter(
        Alert.area_desc.ilike(f'%{state}%')
    ).order_by(Alert.ingested_at.desc())
    
    active_only = request.args.get('active_only', 'false').lower() == 'true'
    if active_only:
        from datetime import datetime
        now = datetime.utcnow()
        query = query.filter(
            Alert.effective <= now,
            Alert.expires > now
        )
    
    alerts = query.all()
    
    return jsonify({
        'state': state,
        'total_alerts': len(alerts),
        'alerts': [alert.to_dict() for alert in alerts]
    })

@app.route('/api/alerts/by-county/<state>/<county>')
def get_alerts_by_county(state, county):
    """Get alerts for a specific county"""
    query = Alert.query.filter(
        Alert.area_desc.ilike(f'%{county}%'),
        Alert.area_desc.ilike(f'%{state}%')
    ).order_by(Alert.ingested_at.desc())
    
    active_only = request.args.get('active_only', 'false').lower() == 'true'
    if active_only:
        from datetime import datetime
        now = datetime.utcnow()
        query = query.filter(
            Alert.effective <= now,
            Alert.expires > now
        )
    
    alerts = query.all()
    
    return jsonify({
        'state': state,
        'county': county,
        'total_alerts': len(alerts),
        'alerts': [alert.to_dict() for alert in alerts]
    })

@app.route('/api/alerts/active')
def get_active_alerts():
    """Get all currently active alerts"""
    from datetime import datetime
    now = datetime.utcnow()
    
    alerts = Alert.query.filter(
        Alert.effective <= now,
        Alert.expires > now
    ).order_by(Alert.severity.desc(), Alert.ingested_at.desc()).all()
    
    return jsonify({
        'timestamp': now.isoformat(),
        'total_active': len(alerts),
        'alerts': [alert.to_dict() for alert in alerts]
    })

@app.route('/api/alerts/search')
def search_alerts():
    """Advanced search endpoint for external applications"""
    # Location parameters
    state = request.args.get('state')
    county = request.args.get('county')
    area = request.args.get('area')
    
    # Alert parameters
    severity = request.args.get('severity')
    event_type = request.args.get('event_type')
    active_only = request.args.get('active_only', 'false').lower() == 'true'
    
    # Pagination
    page = request.args.get('page', 1, type=int)
    limit = min(request.args.get('limit', 50, type=int), 100)
    
    query = Alert.query
    
    # Apply filters
    if state:
        query = query.filter(Alert.area_desc.ilike(f'%{state}%'))
    if county:
        query = query.filter(Alert.area_desc.ilike(f'%{county}%'))
    if area:
        query = query.filter(Alert.area_desc.ilike(f'%{area}%'))
    if severity:
        query = query.filter(Alert.severity == severity)
    if event_type:
        query = query.filter(Alert.event.ilike(f'%{event_type}%'))
    
    if active_only:
        from datetime import datetime
        now = datetime.utcnow()
        query = query.filter(
            Alert.effective <= now,
            Alert.expires > now
        )
    
    # Execute query with pagination
    total = query.count()
    alerts = query.order_by(Alert.ingested_at.desc()).offset((page - 1) * limit).limit(limit).all()
    
    return jsonify({
        'total': total,
        'page': page,
        'limit': limit,
        'pages': (total + limit - 1) // limit,
        'filters': {
            'state': state,
            'county': county,
            'area': area,
            'severity': severity,
            'event_type': event_type,
            'active_only': active_only
        },
        'alerts': [alert.to_dict() for alert in alerts]
    })

@app.route('/alerts/enrich/<alert_id>', methods=['POST'])
def enrich_alert(alert_id):
    """Re-run enrichment manually"""
    alert = Alert.query.get_or_404(alert_id)
    
    try:
        enrich_service.enrich_alert(alert)
        db.session.commit()
        flash(f'Alert {alert_id} enriched successfully', 'success')
    except Exception as e:
        logger.error(f"Error enriching alert {alert_id}: {e}")
        flash(f'Error enriching alert: {str(e)}', 'error')
    
    return redirect(url_for('get_alert', alert_id=alert_id))

@app.route('/api/alerts/enrich-batch', methods=['POST'])
def enrich_batch():
    """Enrich a batch of unenriched alerts"""
    try:
        limit = request.json.get('limit', 50) if request.json else 50
        limit = min(limit, 100)  # Cap at 100 for safety
        
        logger.info(f"Starting batch enrichment for up to {limit} alerts")
        
        result = enrich_service.enrich_batch(limit)
        
        return jsonify({
            'status': 'success',
            'enriched': result['enriched'],
            'failed': result['failed'],
            'total_processed': result['total_processed'],
            'message': f"Successfully enriched {result['enriched']} alerts"
        })
        
    except Exception as e:
        logger.error(f"Error during batch enrichment: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/spc/reports')
def get_spc_reports():
    """Get SPC storm reports with filtering"""
    try:
        # Get query parameters
        report_type = request.args.get('type')  # tornado, wind, hail
        state = request.args.get('state')
        county = request.args.get('county')
        date = request.args.get('date')  # YYYY-MM-DD format
        limit = min(int(request.args.get('limit', 100)), 500)
        offset = int(request.args.get('offset', 0))
        
        # Build query
        query = SPCReport.query
        
        if report_type:
            query = query.filter(SPCReport.report_type == report_type)
        if state:
            query = query.filter(SPCReport.state == state.upper())
        if county:
            query = query.filter(SPCReport.county.ilike(f'%{county}%'))
        if date:
            query = query.filter(SPCReport.report_date == date)
        
        # Get total count for pagination
        total_count = query.count()
        
        # Get results with pagination
        reports = query.order_by(SPCReport.report_date.desc(), SPCReport.time_utc.desc()).limit(limit).offset(offset).all()
        
        return jsonify({
            'reports': [report.to_dict() for report in reports],
            'pagination': {
                'total': total_count,
                'limit': limit,
                'offset': offset,
                'has_more': offset + limit < total_count
            },
            'filters': {
                'type': report_type,
                'state': state,
                'county': county,
                'date': date
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting SPC reports: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/spc/reports')
def view_spc_reports():
    """View SPC reports in web interface"""
    try:
        # Get recent reports for display
        reports = SPCReport.query.order_by(
            SPCReport.report_date.desc(), 
            SPCReport.time_utc.desc()
        ).limit(50).all()
        
        # Get summary stats
        total_reports = SPCReport.query.count()
        type_counts = db.session.query(
            SPCReport.report_type,
            db.func.count(SPCReport.id).label('count')
        ).group_by(SPCReport.report_type).all()
        
        return render_template('spc_reports.html', 
                             reports=reports,
                             total_reports=total_reports,
                             type_counts={row.report_type: row.count for row in type_counts})
        
    except Exception as e:
        logger.error(f"Error viewing SPC reports: {e}")
        return render_template('error.html', error=str(e))

# Internal/Admin Routes
@app.route('/internal/status')
def internal_status():
    """Health status endpoint - comprehensive system diagnostics"""
    try:
        # Basic alert metrics
        total_alerts = Alert.query.count()
        recent_alerts = Alert.query.filter(
            Alert.ingested_at >= datetime.utcnow() - timedelta(hours=24)
        ).count()
        active_alerts = Alert.query.filter(
            Alert.effective <= datetime.utcnow(),
            Alert.expires > datetime.utcnow()
        ).count()
        
        # SPC verification metrics
        verified_alerts = Alert.query.filter(Alert.spc_verified == True).count()
        unverified_alerts = Alert.query.filter(Alert.spc_verified == False).count()
        verification_coverage = (verified_alerts / total_alerts * 100) if total_alerts > 0 else 0
        
        # Last ingestion timestamps
        last_alert = Alert.query.order_by(Alert.ingested_at.desc()).first()
        last_nws_ingestion = last_alert.ingested_at.isoformat() if last_alert else None
        
        # SPC ingestion status
        last_spc_log = SPCIngestionLog.query.order_by(SPCIngestionLog.started_at.desc()).first()
        last_spc_ingestion = last_spc_log.started_at.isoformat() if last_spc_log else None
        
        # Oldest unverified alert (backlog indicator)
        oldest_unverified = Alert.query.filter(
            Alert.spc_verified == False
        ).order_by(Alert.effective.asc()).first()
        oldest_unverified_date = oldest_unverified.effective.isoformat() if oldest_unverified else None
        
        # Recent ingestion logs (error detection)
        recent_logs = SPCIngestionLog.query.filter(
            SPCIngestionLog.started_at >= datetime.utcnow() - timedelta(hours=24)
        ).order_by(SPCIngestionLog.started_at.desc()).limit(10).all()
        
        failed_jobs = [log for log in recent_logs if not log.success]
        
        # Scheduler operation statistics
        scheduler_stats = scheduler_service.get_operation_stats() if scheduler_service else {}
        
        # Database health check
        try:
            db.session.execute(db.text('SELECT 1'))
            db_status = "healthy"
        except Exception as e:
            db_status = f"error: {str(e)}"
        
        return jsonify({
            'status': 'healthy' if len(failed_jobs) == 0 and db_status == "healthy" else 'warning',
            'timestamp': datetime.utcnow().isoformat(),
            'database': db_status,
            'alerts': {
                'total': total_alerts,
                'recent_24h': recent_alerts,
                'active_now': active_alerts
            },
            'spc_verification': {
                'verified_count': verified_alerts,
                'unverified_count': unverified_alerts,
                'coverage_percentage': round(verification_coverage, 2),
                'oldest_unverified': oldest_unverified_date
            },
            'ingestion': {
                'last_nws_ingestion': last_nws_ingestion,
                'last_spc_ingestion': last_spc_ingestion,
                'failed_jobs_24h': len(failed_jobs)
            },
            'system': {
                'environment': 'replit',
                'python_version': '3.11',
                'framework': 'flask+sqlalchemy'
            },
            'scheduler_operations': scheduler_stats
        })
        
    except Exception as e:
        logger.error(f"Error in status endpoint: {e}")
        return jsonify({
            'status': 'error',
            'timestamp': datetime.utcnow().isoformat(),
            'error': str(e)
        }), 500

@app.route('/internal/dashboard')
def internal_dashboard():
    """Admin dashboard"""
    try:
        # Get comprehensive stats
        total_alerts = Alert.query.count()
        enriched_alerts = Alert.query.filter(Alert.ai_summary.isnot(None)).count()
        
        # Recent activity
        recent_alerts = Alert.query.filter(
            Alert.ingested_at >= datetime.utcnow() - timedelta(hours=24)
        ).count()
        
        # Severity breakdown
        severity_stats = db.session.query(
            Alert.severity, db.func.count(Alert.id)
        ).group_by(Alert.severity).all()
        
        # SPC Events statistics
        spc_total_reports = SPCReport.query.count()
        spc_tornado = SPCReport.query.filter(SPCReport.report_type == 'tornado').count()
        spc_wind = SPCReport.query.filter(SPCReport.report_type == 'wind').count()
        spc_hail = SPCReport.query.filter(SPCReport.report_type == 'hail').count()
        
        # Get actual alert types from database for dropdown
        alert_types = db.session.query(Alert.event).distinct().order_by(Alert.event).all()
        alert_types_list = [row[0] for row in alert_types if row[0]]
        
        # Daily totals for last 7 days for alerts
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        alert_daily_totals = db.session.query(
            db.func.date(Alert.ingested_at).label('date'),
            db.func.count(Alert.id).label('count')
        ).filter(
            Alert.ingested_at >= seven_days_ago
        ).group_by(
            db.func.date(Alert.ingested_at)
        ).order_by('date').all()
        
        # Daily totals for last 7 days for SPC events
        spc_daily_totals = db.session.query(
            SPCReport.report_date,
            SPCReport.report_type,
            db.func.count(SPCReport.id).label('count')
        ).filter(
            SPCReport.report_date >= seven_days_ago.date()
        ).group_by(
            SPCReport.report_date,
            SPCReport.report_type
        ).order_by(SPCReport.report_date).all()
        
        # Last ingestion
        last_alert = Alert.query.order_by(Alert.ingested_at.desc()).first()
        
        stats = {
            'total_alerts': total_alerts,
            'enriched_alerts': enriched_alerts,
            'recent_alerts_24h': recent_alerts,
            'spc_total_reports': spc_total_reports,
            'spc_tornado': spc_tornado,
            'spc_wind': spc_wind,
            'spc_hail': spc_hail,
            'alert_types': alert_types_list,
            'alert_daily_totals': [(row.date.strftime('%Y-%m-%d'), row.count) for row in alert_daily_totals],
            'spc_daily_totals': [(row.report_date.strftime('%Y-%m-%d'), row.report_type, row.count) for row in spc_daily_totals],
            'last_ingestion': last_alert.ingested_at if last_alert else None,
            'scheduler_running': scheduler.running if scheduler else False
        }
        
        return render_template('dashboard.html', stats=stats)
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        flash(f'Error loading dashboard: {str(e)}', 'error')
        return render_template('dashboard.html', stats={})

@app.route('/internal/cron', methods=['POST'])
def internal_cron():
    """Enable/disable polling, update interval"""
    action = request.json.get('action') if request.json else None
    
    if action == 'start':
        if scheduler and not scheduler.running:
            scheduler.start()
            return jsonify({'status': 'started'})
    elif action == 'stop':
        if scheduler and scheduler.running:
            scheduler.shutdown()
            return jsonify({'status': 'stopped'})
    elif action == 'trigger':
        # Manual trigger
        try:
            count = ingest_service.poll_nws_alerts()
            return jsonify({'status': 'triggered', 'ingested_count': count})
        except Exception as e:
            logger.error(f"Error triggering ingestion: {e}")
            return jsonify({'status': 'error', 'message': str(e)}), 500
    
    return jsonify({'status': 'no_action'})

@app.route('/internal/metrics')
def internal_metrics():
    """Alert metrics"""
    try:
        metrics = {
            'total_alerts': Alert.query.count(),
            'enriched_alerts': Alert.query.filter(Alert.ai_summary.isnot(None)).count(),
            'active_alerts': Alert.query.filter(
                Alert.expires > datetime.utcnow()
            ).count(),
            'recent_24h': Alert.query.filter(
                Alert.ingested_at >= datetime.utcnow() - timedelta(hours=24)
            ).count(),
            'recent_7d': Alert.query.filter(
                Alert.ingested_at >= datetime.utcnow() - timedelta(days=7)
            ).count()
        }
        
        return jsonify(metrics)
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return jsonify({'error': str(e)}), 500

# Initialize scheduler
def init_scheduler():
    global scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=lambda: ingest_service.poll_nws_alerts(),
        trigger="interval",
        minutes=5,
        id='nws_ingestion'
    )
    scheduler.start()
    logger.info("Scheduler started - polling every 5 minutes")

# Shutdown scheduler when app stops
atexit.register(lambda: scheduler.shutdown() if scheduler else None)

@app.route('/internal/spc-verify')
def spc_verify():
    """SPC Data Integrity Verification"""
    try:
        verification_service = SPCVerificationService(db.session)
        
        # Get date range from query params
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        
        if start_date_str and end_date_str:
            # Use provided date range
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        else:
            # Default to last 7 days
            days = request.args.get('days', 7, type=int)
            end_date = datetime.utcnow().date()
            start_date = end_date - timedelta(days=days-1)
        
        # Run verification
        results = verification_service.verify_date_range(start_date, end_date)
        summary = verification_service.get_verification_summary(results)
        
        if request.args.get('format') == 'json':
            return jsonify({
                'results': results,
                'summary': summary,
                'date_range': {
                    'start': start_date.strftime('%Y-%m-%d'),
                    'end': end_date.strftime('%Y-%m-%d')
                }
            })
        
        return render_template('spc_verification.html', 
                             results=results, 
                             summary=summary,
                             start_date=start_date,
                             end_date=end_date)
    
    except Exception as e:
        logger.error(f"Error in SPC verification: {e}")
        return render_template('error.html', error=str(e)), 500

@app.route('/internal/spc-reupload/<date_str>', methods=['POST'])
def spc_reupload(date_str):
    """Trigger SPC data re-upload for a specific date"""
    try:
        check_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        verification_service = SPCVerificationService(db.session)
        result = verification_service.trigger_reupload_for_date(check_date)
        
        return jsonify(result)
    
    except ValueError:
        return jsonify({'success': False, 'message': 'Invalid date format'}), 400
    except Exception as e:
        logger.error(f"Error re-uploading SPC data for {date_str}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/internal/spc-ingest', methods=['POST'])
def spc_ingest():
    """Trigger SPC report ingestion"""
    try:
        log_entry = scheduler_service.log_operation_start("spc_poll", "manual")
        
        # Poll for last 7 days
        from datetime import datetime, timedelta
        total_reports = 0
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=6)
        
        current_date = start_date
        while current_date <= end_date:
            result = spc_ingest_service.poll_spc_reports(current_date)
            total_reports += result.get('total_reports', 0)
            current_date += timedelta(days=1)
        
        scheduler_service.log_operation_complete(
            log_entry, True, total_reports, total_reports
        )
        
        return jsonify({
            'success': True,
            'total_reports': total_reports,
            'message': f'SPC ingestion completed: {total_reports} reports processed'
        })
        
    except Exception as e:
        logger.error(f"SPC ingestion failed: {e}")
        if 'log_entry' in locals():
            scheduler_service.log_operation_complete(
                log_entry, False, 0, 0, str(e)
            )
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/internal/spc-match', methods=['POST'])
def spc_match():
    """Trigger SPC matching process"""
    try:
        log_entry = scheduler_service.log_operation_start("spc_match", "manual")
        
        result = spc_matching_service.match_spc_reports_batch(limit=100)
        processed = result.get('processed', 0)
        matched = result.get('matched', 0)
        
        scheduler_service.log_operation_complete(
            log_entry, True, processed, matched
        )
        
        return jsonify({
            'success': True,
            'processed': processed,
            'matched': matched,
            'message': f'SPC matching completed: {matched}/{processed} alerts matched'
        })
        
    except Exception as e:
        logger.error(f"SPC matching failed: {e}")
        if 'log_entry' in locals():
            scheduler_service.log_operation_complete(
                log_entry, False, 0, 0, str(e)
            )
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/internal/spc-generate-summaries', methods=['POST'])
def generate_ai_summaries():
    """Generate AI summaries for verified matches without summaries"""
    from match_summarizer import MatchSummarizer
    
    try:
        # Get verified alerts without AI summaries
        alerts = Alert.query.filter(
            Alert.spc_verified == True,
            Alert.spc_ai_summary.is_(None)
        ).limit(50).all()
        
        if not alerts:
            return jsonify({
                'success': True,
                'message': 'No verified matches need AI summaries',
                'generated': 0
            })
        
        summarizer = MatchSummarizer()
        generated = 0
        
        for alert in alerts:
            if alert.spc_reports:
                try:
                    summary = summarizer.generate_match_summary(
                        alert=alert.to_dict(),
                        spc_reports=alert.spc_reports
                    )
                    if summary:
                        alert.spc_ai_summary = summary
                        generated += 1
                except Exception as e:
                    logger.warning(f"Failed to generate summary for alert {alert.id}: {e}")
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': f'Generated {generated} AI summaries for verified matches',
            'generated': generated,
            'total_processed': len(alerts)
        })
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"AI summary generation failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/internal/spc-generate-summary/<alert_id>', methods=['POST'])
def generate_single_ai_summary(alert_id):
    """Generate AI summary for a specific verified alert match"""
    from match_summarizer import MatchSummarizer
    
    try:
        # Get the specific alert
        alert = Alert.query.filter_by(id=alert_id).first()
        
        if not alert:
            return jsonify({'success': False, 'error': 'Alert not found'}), 404
        
        if not alert.spc_verified:
            return jsonify({'success': False, 'error': 'Alert is not SPC verified'}), 400
        
        if not alert.spc_reports:
            return jsonify({'success': False, 'error': 'No SPC reports linked to this alert'}), 400
        
        # Generate AI summary
        summarizer = MatchSummarizer()
        summary = summarizer.generate_match_summary(
            alert=alert.to_dict(),
            spc_reports=alert.spc_reports
        )
        
        if summary:
            alert.spc_ai_summary = summary
            db.session.commit()
            
            return jsonify({
                'success': True,
                'message': 'AI summary generated successfully',
                'summary': summary
            })
        else:
            return jsonify({'success': False, 'error': 'Failed to generate AI summary'}), 500
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Single AI summary generation failed for alert {alert_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/internal/scheduler/start', methods=['POST'])
def start_autonomous_scheduler():
    """Start autonomous scheduler"""
    try:
        autonomous_scheduler.start()
        return jsonify({
            'success': True,
            'status': 'running',
            'message': 'Autonomous scheduler started'
        })
    except Exception as e:
        logger.error(f"Failed to start autonomous scheduler: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/internal/scheduler/stop', methods=['POST'])
def stop_autonomous_scheduler():
    """Stop autonomous scheduler"""
    try:
        autonomous_scheduler.stop()
        return jsonify({
            'success': True,
            'status': 'stopped',
            'message': 'Autonomous scheduler stopped'
        })
    except Exception as e:
        logger.error(f"Failed to stop autonomous scheduler: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/internal/scheduler/status')
def get_autonomous_scheduler_status():
    """Get autonomous scheduler status with countdown information"""
    try:
        status = autonomous_scheduler.get_status()
        
        # Calculate time until next operations
        from datetime import datetime, timedelta
        current_time = datetime.utcnow()
        
        # Next NWS poll (at exact 5-minute intervals: 0, 5, 10, 15, etc.)
        current_minute = current_time.minute
        next_minute = ((current_minute // 5) + 1) * 5
        if next_minute >= 60:
            next_nws = current_time.replace(hour=(current_time.hour + 1) % 24, minute=0, second=0, microsecond=0)
        else:
            next_nws = current_time.replace(minute=next_minute, second=0, microsecond=0)
        nws_countdown = max(0, int((next_nws - current_time).total_seconds()))
        
        # Next SPC poll (every 30 minutes) 
        last_spc = status.get('last_spc_poll')
        if last_spc:
            last_spc_dt = datetime.fromisoformat(last_spc.replace('Z', '+00:00')) if isinstance(last_spc, str) else last_spc
            next_spc = last_spc_dt + timedelta(minutes=30)
            spc_countdown = max(0, int((next_spc - current_time).total_seconds()))
        else:
            spc_countdown = 0
            
        # Next matching (every 15 minutes)
        last_match = status.get('last_matching')
        if last_match:
            last_match_dt = datetime.fromisoformat(last_match.replace('Z', '+00:00')) if isinstance(last_match, str) else last_match
            next_match = last_match_dt + timedelta(minutes=15)
            match_countdown = max(0, int((next_match - current_time).total_seconds()))
        else:
            match_countdown = 0
        
        # Determine which operation is next
        next_operation = "nws"
        next_countdown = nws_countdown
        if spc_countdown < next_countdown and spc_countdown > 0:
            next_operation = "spc"
            next_countdown = spc_countdown
        if match_countdown < next_countdown and match_countdown > 0:
            next_operation = "matching"
            next_countdown = match_countdown
            
        status['next_operation'] = next_operation
        status['next_countdown'] = next_countdown
        status['nws_countdown'] = nws_countdown
        status['spc_countdown'] = spc_countdown
        status['match_countdown'] = match_countdown
        
        return jsonify({
            'success': True,
            'scheduler': status
        })
    except Exception as e:
        logger.error(f"Failed to get scheduler status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/internal/spc-verify-today')
def spc_verify_today():
    """Get recent SPC verification data for dashboard"""
    try:
        from datetime import date, timedelta
        import requests
        
        today = date.today()
        
        # Check last 7 days including today
        verification_results = []
        
        for days_back in range(7):  # Check last 7 days
            check_date = today - timedelta(days=days_back)
            
            # Get HailyDB count for this date
            hailydb_count = SPCReport.query.filter(SPCReport.report_date == check_date).count()
            
            # Get live SPC count by fetching the CSV
            date_str = check_date.strftime("%y%m%d")
            url = f"https://www.spc.noaa.gov/climo/reports/{date_str}_rpts_filtered.csv"
            
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                # Count total data rows (subtract 3 for headers)
                lines = response.text.strip().split('\n')
                total_lines = len(lines)
                spc_live_count = max(0, total_lines - 3)
                
                match_status = 'MATCH' if hailydb_count == spc_live_count else 'MISMATCH'
                
                verification_results.append({
                    'date': check_date.strftime('%Y-%m-%d'),
                    'hailydb_count': hailydb_count,
                    'spc_live_count': spc_live_count,
                    'match_status': match_status
                })
                
            except requests.RequestException:
                # SPC file not available for this date - always show for reference
                verification_results.append({
                    'date': check_date.strftime('%Y-%m-%d'),
                    'hailydb_count': hailydb_count,
                    'spc_live_count': None,
                    'match_status': 'PENDING' if check_date == today else 'UNKNOWN'
                })
        
        return jsonify({
            'status': 'success',
            'results': verification_results,
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in SPC verification: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/api/spc/calendar-verification')
def spc_calendar_verification():
    """Get 60-day SPC verification data for calendar view"""
    try:
        from datetime import date, timedelta
        import requests
        
        # Get offset parameter for navigation (0 = current 60 days, -1 = previous 60 days, etc.)
        offset = int(request.args.get('offset', 0))
        
        # Calculate date range based on offset for historical data display
        # offset = 0: March/April 2025 (60 days ending April 30, 2025)
        # offset = -1: Previous 60 days before that
        # offset = 1: Next 60 days after that
        base_end_date = date(2025, 4, 30)  # Fixed end date for March/April display
        end_date = base_end_date + timedelta(days=offset * 60)
        start_date = end_date - timedelta(days=59)  # 60 days total
        
        verification_results = []
        current_date = start_date
        
        # Use batch database query for better performance
        all_reports = {}
        date_range_reports = SPCReport.query.filter(
            SPCReport.report_date >= start_date,
            SPCReport.report_date <= end_date
        ).all()
        
        # Group by date for fast lookup
        for report in date_range_reports:
            date_key = report.report_date.strftime('%Y-%m-%d')
            if date_key not in all_reports:
                all_reports[date_key] = 0
            all_reports[date_key] += 1
        
        while current_date <= end_date:
            date_key = current_date.strftime('%Y-%m-%d')
            hailydb_count = all_reports.get(date_key, 0)
            
            # Get live SPC count by fetching the CSV with shorter timeout
            date_str = current_date.strftime("%y%m%d")
            url = f"https://www.spc.noaa.gov/climo/reports/{date_str}_rpts_filtered.csv"
            
            try:
                response = requests.get(url, timeout=5)  # Reduced timeout
                response.raise_for_status()
                
                # Count total data rows (subtract 3 for headers)
                lines = response.text.strip().split('\n')
                total_lines = len(lines)
                spc_live_count = max(0, total_lines - 3)
                
                match_status = 'MATCH' if hailydb_count == spc_live_count else 'MISMATCH'
                
                verification_results.append({
                    'date': date_key,
                    'day': current_date.day,
                    'hailydb_count': hailydb_count,
                    'spc_live_count': spc_live_count,
                    'match_status': match_status
                })
                
            except requests.RequestException:
                # SPC file not available for this date
                verification_results.append({
                    'date': date_key,
                    'day': current_date.day,
                    'hailydb_count': hailydb_count,
                    'spc_live_count': None,
                    'match_status': 'PENDING' if current_date == end_date else 'UNAVAILABLE'
                })
            
            current_date += timedelta(days=1)
        
        return jsonify({
            'status': 'success',
            'results': verification_results,
            'date_range': {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            },
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in SPC calendar verification: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

# Home route
@app.route('/')
def index():
    return redirect(url_for('internal_dashboard'))

@app.route('/ingestion-logs')
def ingestion_logs():
    """View ingestion logs page"""
    return render_template('ingestion_logs.html')

@app.route('/ingestion-logs/data')
def ingestion_logs_data():
    """API endpoint for ingestion logs data"""
    try:
        hours = int(request.args.get('hours', 24))
        operation_type = request.args.get('operation_type', '')
        success_param = request.args.get('success', '')
        
        # Build query
        query = SchedulerLog.query
        
        # Filter by time
        since = datetime.utcnow() - timedelta(hours=hours)
        query = query.filter(SchedulerLog.started_at >= since)
        
        # Filter by operation type
        if operation_type:
            query = query.filter(SchedulerLog.operation_type == operation_type)
        
        # Filter by success status
        if success_param == 'true':
            query = query.filter(SchedulerLog.success == True)
        elif success_param == 'false':
            query = query.filter(SchedulerLog.success == False)
        
        # Order by most recent first
        logs = query.order_by(SchedulerLog.started_at.desc()).limit(100).all()
        
        # Calculate summary statistics
        summary_query = SchedulerLog.query.filter(SchedulerLog.started_at >= since)
        if operation_type:
            summary_query = summary_query.filter(SchedulerLog.operation_type == operation_type)
        
        all_logs = summary_query.all()
        success_count = sum(1 for log in all_logs if log.success)
        error_count = len(all_logs) - success_count
        total_processed = sum(log.records_processed or 0 for log in all_logs if log.success)
        total_new = sum(log.records_new or 0 for log in all_logs if log.success)
        
        # Format logs for JSON response
        formatted_logs = []
        for log in logs:
            duration = None
            if log.started_at and log.completed_at:
                duration = round((log.completed_at - log.started_at).total_seconds(), 1)
            
            formatted_logs.append({
                'started_at': log.started_at.isoformat() if log.started_at else None,
                'completed_at': log.completed_at.isoformat() if log.completed_at else None,
                'operation_type': log.operation_type,
                'trigger_method': log.trigger_method,
                'success': log.success,
                'records_processed': log.records_processed,
                'records_new': log.records_new,
                'error_message': log.error_message,
                'duration': duration
            })
        
        return jsonify({
            'summary': {
                'success_count': success_count,
                'error_count': error_count,
                'total_processed': total_processed,
                'total_new': total_new
            },
            'logs': formatted_logs
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/spc-matches')
def spc_matches():
    """View SPC verified matches page"""
    return render_template('spc_matches.html')

@app.route('/spc-matches/data')
def spc_matches_data():
    """API endpoint for SPC verified matches data"""
    try:
        hours = int(request.args.get('hours', 168))  # Default to 7 days
        event_filter = request.args.get('event', '')
        method_filter = request.args.get('method', '')
        confidence_filter = request.args.get('confidence', '')
        state_filter = request.args.get('state', '')
        
        # Build query for verified alerts
        query = Alert.query.filter(Alert.spc_verified == True)
        
        # Filter by time
        since = datetime.utcnow() - timedelta(hours=hours)
        query = query.filter(Alert.effective >= since)
        
        # Filter by event type
        if event_filter:
            query = query.filter(Alert.event == event_filter)
        
        # Filter by match method
        if method_filter:
            query = query.filter(Alert.spc_match_method == method_filter)
        
        # Filter by confidence level
        if confidence_filter == 'high':
            query = query.filter(Alert.spc_confidence_score >= 0.8)
        elif confidence_filter == 'medium':
            query = query.filter(Alert.spc_confidence_score >= 0.5, Alert.spc_confidence_score < 0.8)
        elif confidence_filter == 'low':
            query = query.filter(Alert.spc_confidence_score < 0.5)
        
        # Filter by state (extract from area_desc)
        if state_filter:
            query = query.filter(Alert.area_desc.contains(state_filter))
        
        # Order by most recent first
        matches = query.order_by(Alert.effective.desc()).limit(100).all()
        
        # Calculate summary statistics
        total_alerts = Alert.query.filter(Alert.effective >= since).count()
        verified_count = len(matches) if not any([event_filter, method_filter, confidence_filter, state_filter]) else query.count()
        total_reports = sum(match.spc_report_count or 0 for match in matches)
        high_confidence_count = sum(1 for match in matches if (match.spc_confidence_score or 0) >= 0.8)
        verification_rate = round((verified_count / total_alerts * 100) if total_alerts > 0 else 0, 1)
        
        # Get unique states for filter dropdown
        states = set()
        for match in matches:
            if match.area_desc:
                # Extract state codes from area description
                import re
                state_matches = re.findall(r'\b([A-Z]{2})\b', match.area_desc)
                states.update(state_matches)
        
        # Format matches for JSON response
        formatted_matches = []
        for match in matches:
            # Parse SPC reports
            spc_reports = []
            if match.spc_reports:
                for report in match.spc_reports:
                    spc_reports.append({
                        'report_type': report.get('report_type', 'unknown'),
                        'time_utc': report.get('time_utc', ''),
                        'location': report.get('location', ''),
                        'county': report.get('county', ''),
                        'state': report.get('state', ''),
                        'comments': report.get('comments', '')
                    })
            
            formatted_matches.append({
                'id': match.id,
                'effective': match.effective.isoformat() if match.effective else None,
                'event': match.event,
                'area_desc': match.area_desc,
                'match_method': match.spc_match_method or 'unknown',
                'confidence': match.spc_confidence_score or 0,
                'report_count': match.spc_report_count or 0,
                'spc_reports': spc_reports,
                'spc_ai_summary': match.spc_ai_summary
            })
        
        return jsonify({
            'summary': {
                'verified_count': verified_count,
                'total_reports': total_reports,
                'high_confidence_count': high_confidence_count,
                'verification_rate': verification_rate
            },
            'matches': formatted_matches,
            'states': sorted(list(states))
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    with app.app_context():
        init_scheduler()
    app.run(host='0.0.0.0', port=5000, debug=True)
