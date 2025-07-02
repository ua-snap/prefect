#
# This is an example VCL file for Varnish.
#
# It does not do anything by default, delegating control to the
# builtin VCL. The builtin VCL is called when there is no explicit
# return statement.
#
# See the VCL chapters in the Users Guide at https://www.varnish-cache.org/docs/
# and http://varnish-cache.org/trac/wiki/VCLExamples for more examples.

# Marker to tell the VCL compiler that this VCL has been adapted to the
# new 4.0 format.
# Original API IP address: 52.24.55.121
#
vcl 4.0;

# Default backend definition. Set this to point to your content server.
backend default {
    .host = "52.39.174.198";
    .port = "80";
    .between_bytes_timeout = 900s;
    .first_byte_timeout = 900s;
}

sub vcl_recv {
    # Happens before we check if we have this in cache already.
    #
    # Typically you clean up the request here, removing cookies you don't need,
    # rewriting the request, etc.

    # Allows for any host pointed at the cache to access the cache as if they were
    # named earthmaps.io
    set req.http.Host = "earthmaps.io";

    if (req.url ~ "^/places.*") {
        return (pass);
    }

    if (req.url ~ "^/boundary.*") {
	return (pass);
    }

    if (req.url ~ "^/fire/.*") {
        return (pass);
    }

    if (req.url ~ "^/seaice/enddate/) {
        return (pass);
    }
}

sub vcl_backend_response {
    # Happens after we have read the response headers from the backend.
    #
    # Here you clean the response headers, removing silly Set-Cookie headers
    # and other mistakes your backend does.
    set beresp.ttl = 104w;

    if (beresp.status == 500 || beresp.status == 502 || beresp.status == 503 || beresp.status == 504) {
        return (retry);
    }
}

sub vcl_deliver {
    # Happens when we have all the pieces we need, and are about to send the
    # response to the client.
    #
    # You can do accounting or modifying the final object here.
    if (obj.hits > 0) {
        set resp.http.X-Cache = "HIT";
    } else {
        set resp.http.X-Cache = "MISS";
    }
}