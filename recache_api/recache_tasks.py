import requests
from prefect import task
from prefect.artifacts import create_markdown_artifact
from collections import defaultdict, Counter
from datetime import datetime


class LogCapture:
    """Captures print statements and collects request/response data for analysis."""
    
    def __init__(self):
        self.requests_data = []
        self.route_stats = defaultdict(lambda: {
            'total': 0,
            'status_codes': Counter(),
            'urls': []
        })
        # Enhanced status code tracking
        self.all_status_codes_global = Counter()
        
    def capture_request(self, url, status_code):
        """Capture request data for analysis."""
        route = self._extract_route(url)
        route = self._validate_extracted_route(route, url)  # Add validation
        request_data = {
            'url': url,
            'route': route,
            'status_code': status_code,
            'timestamp': datetime.now()
        }
        self.requests_data.append(request_data)
        
        # Update route statistics
        self.route_stats[route]['total'] += 1
        self.route_stats[route]['status_codes'][status_code] += 1
        self.route_stats[route]['urls'].append(url)
        
        # Enhanced global status code tracking
        self.all_status_codes_global[status_code] += 1
    
    def _extract_route(self, url: str) -> str:
        """Extract the route/endpoint from a full URL."""
        # Capture everything between earthmaps.io/ and the first standalone number (coordinates/IDs)
        # This handles both coordinates (like 56.6003) and IDs (like 12345)
        # But preserves numbers within words like "cmip5"
        # Examples: 
        # - https://earthmaps.io/beetles/point/56.6003/-133.0267 -> beetles/point
        # - https://earthmaps.io/alfresco/flammability/local/56.6/-133.0 -> alfresco/flammability/local
        # - https://earthmaps.io/indicators/cmip5/point/56.6/-133.0 -> indicators/cmip5/point
        import re
        match = re.search(r'earthmaps\.io/(.+?)/?(?=[-]?\d+\.?\d*/?[-]?\d|[-]?\d+/?$)', url)
        return match.group(1).rstrip('/') if match else 'unknown'
    
    def _validate_extracted_route(self, route: str, original_url: str) -> str:
        """Validate and potentially adjust extracted route for debugging."""
        # Remove any trailing slashes for consistency
        route = route.rstrip('/')
        
        # Log potential issues for debugging (only for routes with >3 segments)
        segments = route.split('/')
        if len(segments) > 3:
            print(f"⚠️ Route has more than 3 segments: {route} from {original_url}")
        
        return route
    
    def _categorize_status_code(self, code: int) -> str:
        """Categorize status code by its class (1xx, 2xx, etc.)."""
        if 100 <= code < 200:
            return "1xx Informational"
        elif 200 <= code < 300:
            return "2xx Success"
        elif 300 <= code < 400:
            return "3xx Redirection"
        elif 400 <= code < 500:
            return "4xx Client Error"
        elif 500 <= code < 600:
            return "5xx Server Error"
        else:
            return "Unknown"
    
    def _get_status_code_meaning(self, code: int) -> str:
        """Get human-readable meaning for status code."""
        meanings = {
            # 1xx Informational
            100: "Continue",
            101: "Switching Protocols",
            102: "Processing",
            103: "Early Hints",
            
            # 2xx Success
            200: "OK",
            201: "Created",
            202: "Accepted",
            204: "No Content",
            206: "Partial Content",
            
            # 3xx Redirection
            300: "Multiple Choices",
            301: "Moved Permanently",
            302: "Found",
            303: "See Other",
            304: "Not Modified",
            307: "Temporary Redirect",
            308: "Permanent Redirect",
            
            # 4xx Client Error
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            408: "Request Timeout",
            409: "Conflict",
            410: "Gone",
            429: "Too Many Requests",
            431: "Request Header Fields Too Large",
            
            # 5xx Server Error
            500: "Internal Server Error",
            501: "Not Implemented",
            502: "Bad Gateway",
            503: "Service Unavailable",
            504: "Gateway Timeout",
            505: "HTTP Version Not Supported"
        }
        return meanings.get(code, f"HTTP {code}")
    
    def get_all_encountered_status_codes(self) -> list:
        """Get sorted list of all unique status codes encountered."""
        return sorted(self.all_status_codes_global.keys())
    
    def get_status_codes_by_category(self) -> dict:
        """Group status codes by their category."""
        categories = defaultdict(list)
        for code in self.get_all_encountered_status_codes():
            category = self._categorize_status_code(code)
            categories[category].append(code)
        return dict(categories)
    
    def get_routes_with_5xx_errors(self) -> dict:
        """Get all routes that have any 5xx server errors with detailed breakdown."""
        routes_with_5xx = {}
        
        for route, stats in self.route_stats.items():
            # Find all 5xx status codes for this route
            route_5xx_codes = {}
            total_5xx = 0
            
            for status_code, count in stats['status_codes'].items():
                if 500 <= status_code < 600:
                    route_5xx_codes[status_code] = count
                    total_5xx += count
            
            # If route has any 5xx errors, add to results
            if total_5xx > 0:
                total_requests = stats['total']
                route_5xx_rate = (total_5xx / total_requests * 100) if total_requests > 0 else 0
                
                # Find most common 5xx code
                most_common_5xx = max(route_5xx_codes.items(), key=lambda x: x[1])[0] if route_5xx_codes else None
                
                routes_with_5xx[route] = {
                    'total_5xx': total_5xx,
                    '5xx_codes': route_5xx_codes,
                    '5xx_rate': route_5xx_rate,
                    'most_common_5xx': most_common_5xx,
                    'total_requests': total_requests
                }
        
        return routes_with_5xx
    
    def get_5xx_summary_stats(self) -> dict:
        """Get summary statistics for all 5xx server errors across all routes."""
        routes_with_5xx = self.get_routes_with_5xx_errors()
        
        # Calculate totals
        total_5xx_errors = sum(self.all_status_codes_global.get(code, 0) 
                              for code in self.all_status_codes_global.keys() 
                              if 500 <= code < 600)
        
        total_requests = len(self.requests_data)
        global_5xx_rate = (total_5xx_errors / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'routes_with_5xx': len(routes_with_5xx),
            'total_5xx_errors': total_5xx_errors,
            'global_5xx_rate': global_5xx_rate,
            'total_routes': len(self.route_stats)
        }
    

    
    def get_5xx_urls_for_route(self, route: str) -> list:
        """Get all URLs that returned 5xx errors for a specific route."""
        fivexx_urls = []
        
        # Filter requests_data for this route with 5xx status codes
        for request in self.requests_data:
            if request['route'] == route and 500 <= request['status_code'] < 600:
                fivexx_urls.append({
                    'url': request['url'],
                    'status_code': request['status_code'],
                    'meaning': self._get_status_code_meaning(request['status_code']),
                    'timestamp': request['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                })
        
        # Sort by timestamp for chronological analysis
        fivexx_urls.sort(key=lambda x: x['timestamp'])
        
        return fivexx_urls
    
    def generate_analysis_markdown(self, completed_routes=None, total_routes=None) -> str:
        """Generate markdown analysis report."""
        if not self.requests_data:
            return "# Recache Analysis\n\nNo requests were captured during this run."
        
        total_requests = len(self.requests_data)
        unique_routes = len(self.route_stats)
        
        # Calculate overall stats
        all_status_codes = Counter()
        for stats in self.route_stats.values():
            all_status_codes.update(stats['status_codes'])
        
        healthy_count = all_status_codes.get(200, 0) + all_status_codes.get(404, 0)
        healthy_response_rate = (healthy_count / total_requests * 100) if total_requests > 0 else 0
        
        # Determine completion status
        completion_status = ""
        if completed_routes is not None and total_routes is not None:
            if completed_routes == total_routes:
                completion_status = "✅ COMPLETED"
            else:
                completion_status = "⚠️ PARTIALLY COMPLETED (CANCELLED)"
        
        # Get 5xx error analysis
        routes_with_5xx = self.get_routes_with_5xx_errors()
        fivexx_summary = self.get_5xx_summary_stats()
        
        # Determine 5xx health indicator
        fivexx_health = ""
        if fivexx_summary['total_5xx_errors'] == 0:
            fivexx_health = "🟢"
        elif fivexx_summary['global_5xx_rate'] < 2 and fivexx_summary['routes_with_5xx'] <= 2:
            fivexx_health = "🟡"
        elif fivexx_summary['global_5xx_rate'] < 5 and fivexx_summary['routes_with_5xx'] <= 5:
            fivexx_health = "🟠"
        else:
            fivexx_health = "🔴"
        
        # Build 5xx summary line for executive summary
        fivexx_summary_line = ""
        if fivexx_summary['total_5xx_errors'] > 0:
            fivexx_summary_line = f"- **5xx Server Errors**: {fivexx_summary['total_5xx_errors']} ({fivexx_summary['global_5xx_rate']:.1f}%) {fivexx_health}\n- **Routes with 5xx Issues**: {fivexx_summary['routes_with_5xx']} / {fivexx_summary['total_routes']}\n"
        
        # Analyze encountered status codes for dynamic table
        all_encountered_codes = self.get_all_encountered_status_codes()
        
        # Determine most important status codes to display (top 5 most frequent)
        code_frequency = [(code, self.all_status_codes_global[code]) for code in all_encountered_codes]
        code_frequency.sort(key=lambda x: x[1], reverse=True)
        
        # Always include common codes if present, then add most frequent others
        priority_codes = [200, 404, 500, 301, 302, 429, 503, 502]
        display_codes = []
        
        # Add priority codes that actually appear
        for code in priority_codes:
            if code in all_encountered_codes:
                display_codes.append(code)
        
        # Add other frequent codes (up to 7 total columns)
        for code, count in code_frequency:
            if code not in display_codes and len(display_codes) < 7:
                display_codes.append(code)
        
        # Generate dynamic table headers
        header_parts = ["Route", "Total"]
        for code in display_codes:
            meaning = self._get_status_code_meaning(code)
            header_parts.append(f"{code} ({meaning})")
        
        # Add 5xx Status column if any routes have 5xx errors
        has_5xx_errors = bool(routes_with_5xx)
        if has_5xx_errors:
            header_parts.append("5xx Status")
        
        header_parts.append("Healthy (200 or 404) Response Rate")
        
        # Generate route summary table with dynamic columns
        table_rows = []
        for route, stats in sorted(self.route_stats.items()):
            total = stats['total']
            healthy_count = stats['status_codes'].get(200, 0) + stats['status_codes'].get(404, 0)
            route_healthy_rate = (healthy_count / total * 100) if total > 0 else 0
            
            # Determine if this route has 5xx errors and add emoji prefix
            route_display_name = route
            fivexx_status = "✅"
            
            if route in routes_with_5xx:
                route_display_name = f"🚨 {route}"
                route_data = routes_with_5xx[route]
                
                # Create 5xx status indicator
                if len(route_data['5xx_codes']) == 1:
                    # Single 5xx error type
                    code, count = list(route_data['5xx_codes'].items())[0]
                    if route_data['5xx_rate'] > 10:
                        fivexx_status = f"🚨 {count}×{code}"
                    else:
                        fivexx_status = f"⚠️ {count}×{code}"
                else:
                    # Multiple 5xx error types
                    total_5xx = route_data['total_5xx']
                    if route_data['5xx_rate'] > 10:
                        fivexx_status = f"🚨 {total_5xx}×5xx"
                    else:
                        fivexx_status = f"⚠️ {total_5xx}×5xx"
            
            # Build row with dynamic status code columns
            row_parts = [route_display_name, str(total)]
            for code in display_codes:
                count = stats['status_codes'].get(code, 0)
                row_parts.append(str(count))
            
            # Add 5xx status column if applicable
            if has_5xx_errors:
                row_parts.append(fivexx_status)
            
            row_parts.append(f"{route_healthy_rate:.1f}%")
            
            table_rows.append("| " + " | ".join(row_parts) + " |")
        
        # Create dynamic table header
        table_header = "| " + " | ".join(header_parts) + " |"
        table_separator = "|" + "|".join(["-" * (len(part) + 2) for part in header_parts]) + "|"
        
        route_table = table_header + '\n' + table_separator + '\n' + '\n'.join(table_rows)
        
        # Generate 5xx Critical Alerts Section
        fivexx_critical_section = ""
        
        # Server Errors (5xx)
        critical_5xx_routes = []
        for route, stats in self.route_stats.items():
            total = stats['total']
            if total == 0:
                continue
            
            # Check for any 5xx errors
            route_5xx_errors = sum(stats['status_codes'].get(code, 0) for code in stats['status_codes'].keys() 
                                  if 500 <= code < 600)
            if route_5xx_errors > 0:
                error_rate = (route_5xx_errors / total * 100)
                
                # Get breakdown of specific 5xx codes
                fivexx_codes_found = []
                for code in [500, 502, 503, 504, 505]:
                    count = stats['status_codes'].get(code, 0)
                    if count > 0:
                        meaning = self._get_status_code_meaning(code)
                        fivexx_codes_found.append(f"{count}×{code} {meaning}")
                
                codes_detail = ", ".join(fivexx_codes_found)
                critical_5xx_routes.append(f"- **🚨 {route}**: Critical server errors - {error_rate:.1f}% 5xx responses ({route_5xx_errors}/{total}) - [{codes_detail}]")
        
        # Generate detailed sections with comprehensive status code analysis
        detailed_sections = []
        for route, stats in sorted(self.route_stats.items()):
            total = stats['total']
            
            # Status breakdown with all codes and meanings
            status_breakdown = []
            
            for status_code in sorted(stats['status_codes'].keys()):
                count = stats['status_codes'][status_code]
                percentage = (count / total * 100) if total > 0 else 0
                meaning = self._get_status_code_meaning(status_code)
                category = self._categorize_status_code(status_code)
                
                # Add emoji indicators based on status code category
                if 200 <= status_code < 300:
                    emoji = "✅"
                elif 300 <= status_code < 400:
                    emoji = "🔄"
                elif 400 <= status_code < 500:
                    emoji = "⚠️"
                elif 500 <= status_code < 600:
                    emoji = "❌"
                else:
                    emoji = "❓"
                
                status_breakdown.append(f"- {emoji} **{status_code} {meaning}** ({category}): {count} requests ({percentage:.1f}%)")
            
            # Route health assessment
            healthy_count = stats['status_codes'].get(200, 0) + stats['status_codes'].get(404, 0)
            route_healthy_rate = (healthy_count / total * 100) if total > 0 else 0
            
            # Enhanced health assessment considering 5xx errors
            route_5xx_errors = sum(stats['status_codes'].get(code, 0) for code in stats['status_codes'].keys() 
                                  if 500 <= code < 600)
            route_5xx_rate = (route_5xx_errors / total * 100) if total > 0 else 0
            
            if route_5xx_rate > 10:
                health_status = "🚨 Critical"
            elif route_5xx_rate > 5:
                health_status = "🔴 Poor"
            elif route_5xx_errors > 0:  # Any 5xx errors
                health_status = "🟠 Fair" if route_healthy_rate >= 70 else "🔴 Poor"
            elif route_healthy_rate >= 95:
                health_status = "🟢 Excellent"
            elif route_healthy_rate >= 85:
                health_status = "🟡 Good"
            elif route_healthy_rate >= 70:
                health_status = "🟠 Fair"
            else:
                health_status = "🔴 Poor"
            
            # Check if this route has 5xx errors and add server error details table
            fivexx_table_section = ""
            if route_5xx_errors > 0:
                fivexx_urls = self.get_5xx_urls_for_route(route)
                
                if fivexx_urls:
                    # Create table with specific URLs that returned 5xx errors
                    table_rows = []
                    for url_data in fivexx_urls:
                        table_rows.append(f"| `{url_data['url']}` | {url_data['status_code']} | {url_data['meaning']} | {url_data['timestamp']} |")
                    
                    fivexx_table_section = f"""
**🚨 Server Error Details**:
| URL | Status Code | Error Type | Timestamp |
|-----|-------------|------------|-----------|
{chr(10).join(table_rows)}
"""
            
            section = f"""### 📍 **{route}**

**Route Health**: {health_status} (Healthy Response Rate: {route_healthy_rate:.1f}%)
**Total Requests**: {total:,}

**Status Code Breakdown**:
{chr(10).join(status_breakdown)}{fivexx_table_section}

---"""
            
            detailed_sections.append(section)
        
        # Build completion status line for executive summary
        completion_line = ""
        if completed_routes is not None and total_routes is not None:
            completion_line = f"- **Completion Status**: {completed_routes}/{total_routes} routes processed\n- **Status**: {completion_status}\n"
        
        # Generate comprehensive status code breakdown by category
        status_categories = self.get_status_codes_by_category()
        status_breakdown_lines = []
        
        for category in ["2xx Success", "3xx Redirection", "4xx Client Error", "5xx Server Error", "1xx Informational"]:
            if category in status_categories:
                codes_in_category = status_categories[category]
                category_total = sum(self.all_status_codes_global[code] for code in codes_in_category)
                category_percent = (category_total / total_requests * 100) if total_requests > 0 else 0
                
                status_breakdown_lines.append(f"├── {category}: {category_total:,} ({category_percent:.1f}%)")
                
                # Show individual codes within category (up to 5 most frequent)
                category_codes = [(code, self.all_status_codes_global[code]) for code in codes_in_category]
                category_codes.sort(key=lambda x: x[1], reverse=True)
                
                for i, (code, count) in enumerate(category_codes[:5]):
                    meaning = self._get_status_code_meaning(code)
                    code_percent = (count / total_requests * 100) if total_requests > 0 else 0
                    prefix = "│   ├──" if i < len(category_codes[:5]) - 1 else "│   └──"
                    status_breakdown_lines.append(f"{prefix} {code} {meaning}: {count:,} ({code_percent:.1f}%)")
                
                # Add "and X more" if there are additional codes
                if len(category_codes) > 5:
                    additional_count = len(category_codes) - 5
                    status_breakdown_lines.append(f"│   └── ... and {additional_count} more")
        
        # Handle any unknown categories
        if "Unknown" in status_categories:
            unknown_codes = status_categories["Unknown"]
            unknown_total = sum(self.all_status_codes_global[code] for code in unknown_codes)
            unknown_percent = (unknown_total / total_requests * 100) if total_requests > 0 else 0
            status_breakdown_lines.append(f"└── Unknown: {unknown_total:,} ({unknown_percent:.1f}%)")
        
        status_breakdown = '\n'.join(status_breakdown_lines)
        
        # Build the complete markdown report
        markdown_report = f"""# 🔄 Prefect Recaching API Analysis

*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*

## **Executive Summary**
- **Total Requests**: {total_requests:,}
{completion_line}{fivexx_summary_line}- **Unique Routes**: {unique_routes}
- **Overall Healthy (200 or 404) Response Rate**: {healthy_response_rate:.1f}%
- **Execution Time**: {(self.requests_data[-1]['timestamp'] - self.requests_data[0]['timestamp']).total_seconds():.1f}s

---
{fivexx_critical_section}
## **Route Performance Overview**

{route_table}

---

## **Detailed Route Analysis**

{chr(10).join(detailed_sections)}

---

## 🚨 **Issues & Alerts**

### 🔥 Server Errors (5xx)
{chr(10).join(critical_5xx_routes) if critical_5xx_routes else "✅ No server errors detected."}

---

## 📋 **Status Code Distribution**

```
Overall Breakdown:
{status_breakdown}
```

---
"""
        
        return markdown_report


def _create_analysis_artifact(log_capture, description_suffix="", completed_routes=None, total_routes=None):
    """Isolated artifact creation function that handles errors gracefully."""
    try:
        # Generate full analysis with completion status
        analysis_markdown = log_capture.generate_analysis_markdown(completed_routes, total_routes)
        
        description = f"Recaching performance analysis{description_suffix}"
        artifact_id = create_markdown_artifact(
            key="recache-analysis",
            markdown=analysis_markdown,
            description=description
        )
        
        return {
            'artifact_id': artifact_id,
            'status': 'success'
        }
        
    except Exception as e:
        # Fallback: create minimal artifact
        print(f"⚠️ Error creating full artifact: {e}")
        return _create_minimal_artifact(log_capture, str(e))


def _create_minimal_artifact(log_capture, error_msg):
    """Create basic artifact if full generation fails."""
    try:
        basic_stats = f"""# 🔄 Recache Analysis (Partial/Error)

*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*

## ⚠️ **Status**
**Error**: {error_msg}

## 📊 **Basic Statistics**
- **Requests Processed**: {len(log_capture.requests_data)}
- **Routes Attempted**: {len(log_capture.route_stats)}

## 📝 **Note**
This is a minimal analysis due to an error during full artifact generation.
The recaching process may have been interrupted or encountered issues.

---

*🤖 Auto-generated by Prefect Recaching Flow (Minimal Mode)*"""
        
        artifact_id = create_markdown_artifact(
            key="recache-analysis-minimal",
            markdown=basic_stats,
            description="Minimal recache analysis due to error"
        )
        
        return {
            'artifact_id': artifact_id, 
            'status': 'minimal'
        }
        
    except Exception as e2:
        print(f"❌ Failed to create even minimal artifact: {e2}")
        return {
            'artifact_id': None, 
            'status': 'failed'
        }



# Global log capture instance
log_capture = LogCapture()


@task(name="Recache API")
def recache_api(cached_urls):
    """Main recaching task that processes all cached URLs and generates analysis."""
    global log_capture
    log_capture = LogCapture()  # Reset for this run
    
    print(f"🚀 Starting recache process for {len(cached_urls)} routes...")
    
    completed_routes = 0
    total_routes = len(cached_urls)
    
    try:
        # Main processing loop
        # consider logging vs. print
        for route in cached_urls:
            print(f"Processing route: {route}")
            if (
                route.find("point") != -1
                or route.find("local") != -1
                or route.find("all") != -1
            ) and (route.find("lat") == -1):
                get_all_route_endpoints(route, "community")
            elif route.find("area") != -1 and route.find("var_id") == -1:
                get_all_route_endpoints(route, "area")
            
            completed_routes += 1
            
    finally:
        # ALWAYS create artifact, regardless of completion status
        if completed_routes == total_routes:
            status_suffix = f" - {completed_routes}/{total_routes} routes completed"
            print(f"✅ Recache process completed. Processed {len(log_capture.requests_data)} total requests.")
        else:
            status_suffix = f" - {completed_routes}/{total_routes} routes completed (CANCELLED)"
            print(f"⚠️ Recache process interrupted. Processed {len(log_capture.requests_data)} total requests from {completed_routes} routes.")
        
        artifact_info = _create_analysis_artifact(log_capture, status_suffix, completed_routes, total_routes)
        
        # Log final status
        print(f"📊 Analysis artifact created: {artifact_info['artifact_id']}")
        
    return {
        "total_requests": len(log_capture.requests_data),
        "unique_routes": len(log_capture.route_stats),
        "completed_routes": completed_routes,
        "total_routes": total_routes,
        "artifact_id": str(artifact_info['artifact_id']) if artifact_info['artifact_id'] else None,
        "status": artifact_info['status']
    }


def get_all_route_endpoints(curr_route, curr_type):
    """Generates all possible endpoints given a particular route & type

    Args:
        curr_route - Current route ex. https://earthmaps.io/taspr/huc/
        curr_type - One of the many types availabe such as community or huc.

    Returns:
        Nothing.
    """
    GS_BASE_URL = "https://gs.earthmaps.io/geoserver/"

    # Opens the JSON file for the current type and replaces the "variable" portions
    # of the route to allow for the JSON items to fill in those fields.
    if curr_type == "community":
        places_url = (
            GS_BASE_URL
            + f"wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=all_boundaries:all_communities&outputFormat=application/json&propertyName=latitude,longitude,region"
        )
        response = requests.get(places_url)
        places_data = response.json()
        places = places_data["features"]
    else:
        places_url = (
            GS_BASE_URL
            + f"wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=all_boundaries:all_areas&outputFormat=application/json&propertyName=id,area_type"
        )
        response = requests.get(places_url)
        places_data = response.json()
        places = places_data["features"]

    # We are excluding HUC12 areas because they add a lot of additional caching
    # that is not used in any of our apps.
    for place in places:
        if (
            curr_type == "community"
            and (
                place["properties"]["region"]
                in ["Alaska", "Yukon", "Northwest Territories"]
            )
        ) or (curr_type == "area" and place["properties"]["area_type"] != "HUC12"):
            get_endpoint(curr_route, curr_type, place["properties"])


def get_endpoint(curr_route, curr_type, place):
    """Requests a specific endpoint of the API with parameters coming from
    the JSON of communities, HUCs, or protected areas.

     Args:
         curr_route - Current route ex. https://earthmaps.io/taspr/huc/
         curr_type - One of three types: community, huc, or pa
         place - One item of the JSON for community, huc, or protected area

     Returns:
         Nothing.

    """
    global log_capture
    
    # Build the URL to query based on type
    if curr_type == "community":
        url = (
            "https://earthmaps.io"
            + curr_route
            + str(place["latitude"])
            + "/"
            + str(place["longitude"])
        )
    else:
        url = "https://earthmaps.io" + curr_route + str(place["id"])

    print(f"Running URL: {url}")

    # Collects returned status from GET request
    status = requests.get(url)
    print(f"Status Response: {status}")
    
    # Capture data for analysis
    log_capture.capture_request(url, status.status_code)
