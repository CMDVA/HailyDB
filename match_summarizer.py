"""
AI-Powered Alert Match Summarizer
Generates intelligent summaries combining NWS alerts with confirmed SPC reports
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Optional
from openai import OpenAI

class MatchSummarizer:
    """
    AI service for generating enhanced summaries of verified alert matches
    Combines NWS alert data with SPC confirmation reports and geographical context
    """
    
    def __init__(self):
        self.openai = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    def generate_match_summary(self, alert: Dict, spc_reports: List[Dict]) -> Optional[str]:
        """
        Generate an AI summary combining alert and confirmation data
        
        Args:
            alert: NWS alert data dictionary
            spc_reports: List of matching SPC report dictionaries
        
        Returns:
            Generated summary string or None if generation fails
        """
        try:
            # Prepare context data
            context = self._prepare_context(alert, spc_reports)
            
            # Generate summary using OpenAI
            prompt = self._build_prompt(context)
            
            response = self.openai.chat.completions.create(
                model="gpt-4o",  # the newest OpenAI model is "gpt-4o" which was released May 13, 2024. do not change this unless explicitly requested by the user
                messages=[
                    {
                        "role": "system",
                        "content": "You are a meteorological analyst specializing in severe weather verification. "
                        + "Generate concise, factual summaries combining weather warnings with confirmed reports. "
                        + "Focus on verification outcomes, precise timing, and geographical context. "
                        + "Use professional meteorological language appropriate for emergency management and research applications."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=300,
                temperature=0.3  # Lower temperature for factual consistency
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            print(f"Error generating match summary: {e}")
            return None
    
    def _prepare_context(self, alert: Dict, spc_reports: List[Dict]) -> Dict:
        """Prepare structured context data for AI processing"""
        
        # Extract key alert information
        alert_context = {
            "event_type": alert.get('event', 'Unknown'),
            "severity": alert.get('severity', 'Unknown'),
            "effective_time": self._format_time(alert.get('effective')),
            "expires_time": self._format_time(alert.get('expires')),
            "area_description": alert.get('area_desc', ''),
            "headline": alert.get('properties', {}).get('headline', ''),
            "description": alert.get('properties', {}).get('description', '')[:500] + "..." if len(alert.get('properties', {}).get('description', '')) > 500 else alert.get('properties', {}).get('description', '')
        }
        
        # Process SPC reports
        reports_context = []
        for report in spc_reports:
            report_context = {
                "type": report.get('type', 'unknown').upper(),
                "time": report.get('time', ''),
                "location": report.get('location', ''),
                "county": report.get('county', ''),
                "state": report.get('state', ''),
                "comments": report.get('comments', ''),
                "magnitude": report.get('magnitude', {})
            }
            reports_context.append(report_context)
        
        return {
            "alert": alert_context,
            "confirmed_reports": reports_context,
            "total_reports": len(reports_context)
        }
    
    def _build_prompt(self, context: Dict) -> str:
        """Build the AI prompt with structured data"""
        
        alert = context["alert"]
        reports = context["confirmed_reports"]
        
        prompt = f"""
Analyze this verified weather event and generate a comprehensive summary:

ORIGINAL WARNING:
- Event: {alert['event_type']} ({alert['severity']})
- Time: {alert['effective_time']} to {alert['expires_time']}
- Area: {alert['area_description']}
- Headline: {alert['headline']}

CONFIRMED REPORTS ({context['total_reports']} total):
"""
        
        for i, report in enumerate(reports, 1):
            magnitude_text = ""
            if report['magnitude']:
                if 'f_scale' in report['magnitude']:
                    magnitude_text = f" (F{report['magnitude']['f_scale']})"
                elif 'speed' in report['magnitude']:
                    magnitude_text = f" ({report['magnitude']['speed']} mph)"
                elif 'size' in report['magnitude']:
                    magnitude_text = f" ({report['magnitude']['size']} inch)"
            
            prompt += f"""
{i}. {report['type']}{magnitude_text} at {report['time']} UTC
   Location: {report['location']}, {report['county']} County, {report['state']}
   Details: {report['comments']}
"""
        
        prompt += """

Generate a summary that:
1. Confirms the warning was verified by actual reports
2. Provides precise timing between warning and events
3. Describes the geographical impact and any progression
4. Mentions nearby areas or communities that may have been affected
5. Includes relevant magnitude/intensity details
6. Uses language like "confirmed reports validate the warning" or "the alert was substantiated by storm reports"

Keep the summary factual, concise (2-3 sentences), and suitable for emergency management professionals.
"""
        
        return prompt
    
    def _format_time(self, time_str: Optional[str]) -> str:
        """Format ISO datetime string to readable format"""
        if not time_str:
            return "Unknown"
        
        try:
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            return dt.strftime("%I:%M %p UTC on %B %d")
        except:
            return time_str
    
    def batch_generate_summaries(self, matches: List[Dict]) -> Dict[str, str]:
        """
        Generate summaries for multiple matches
        
        Args:
            matches: List of match dictionaries with alert and spc_reports
        
        Returns:
            Dictionary mapping alert IDs to generated summaries
        """
        summaries = {}
        
        for match in matches:
            if match.get('spc_reports') and len(match['spc_reports']) > 0:
                summary = self.generate_match_summary(
                    alert=match,
                    spc_reports=match['spc_reports']
                )
                if summary:
                    summaries[match['id']] = summary
        
        return summaries