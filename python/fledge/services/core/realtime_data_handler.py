import json
import asyncio
from datetime import datetime
from aiohttp import web
import logging


# Use a regular dictionary and asyncio.Lock for thread safety in async context
realtime_data_buffer = {}  # asset_name (str) -> list of data entries (dict)
realtime_buffer_mutex = asyncio.Lock()  # Async lock for thread-safe access
MAX_BUFFER_SIZE = 50  # Keep only the last N entries per asset
# ---------------------------------------------------------

_logger = logging.getLogger(__name__)

# --- Real-time Data Endpoint Handlers ---
# Inside realtime_data_handler.py - TEMPORARY DEBUGGING VERSION
async def south_data_post(request):
    """Handle POST data from South plugins to /south-data/{asset} - DEBUG VERSION"""
    # --- Mark request as Core Management related ---
    request.is_core_mgt = True
    # -------------------------------------------------
    try:
        asset_name = request.match_info.get('asset', None)
        if not asset_name:
            raise web.HTTPBadRequest(reason="Asset name is required.")

        # --- SIMPLIFIED LOGIC - NO LOCK, NO BUFFER ---
        # Just log the request and return success
        _logger.info(f"DEBUG: Received POST for asset '{asset_name}'. Returning success immediately.")
        response_data = {
            "status": "debug_success",
            "message": f"Debug: Data received for asset '{asset_name}' (no storage)",
            "asset": asset_name
        }
        return web.json_response(response_data)
        # ---------------------------------------------

    except web.HTTPException:
        raise
    except Exception as ex:
        _logger.exception("DEBUG: Error in simplified south_data_post: %s", str(ex))
        raise web.HTTPInternalServerError(reason=f"Debug error: {str(ex)}")

async def get_realtime_data(request):
    """Handle GET requests to /api/realtime/{asset}"""
    # --- Mark request as Core Management related to bypass standard auth on main API ---
    # Setting this attribute makes this endpoint publicly accessible on the main API port.
    request.is_core_mgt = True
    # -------------------------------------------------------------------------

    try:
        # 1. Extract asset name from the URL path
        asset_name = request.match_info.get('asset', None)
        if not asset_name:
            raise web.HTTPBadRequest(reason="Asset name is required in the URL path.")

        # 2. Retrieve data for the asset (thread-safe)
        async with realtime_buffer_mutex:
            # Get the list of data entries for the asset, or an empty list if none
            data_entries = realtime_data_buffer.get(asset_name, [])

        # 3. Send the data as a JSON response
        _logger.debug(f"Fetched real-time data for asset: {asset_name}")
        return web.json_response(data_entries)

    except web.HTTPException: # Re-raise HTTP exceptions
        raise
    except Exception as ex:
        _logger.exception("Error fetching real-time data for asset %s: %s", asset_name if 'asset_name' in locals() else 'unknown', str(ex))
        raise web.HTTPInternalServerError(reason=f"Internal error fetching data: {str(ex)}")


async def get_all_realtime_data(request):
    """Handle GET requests to /api/realtime (fetch all assets)"""
    # --- Mark request as Core Management related to bypass standard auth on main API ---
    # Setting this attribute makes this endpoint publicly accessible on the main API port.
    request.is_core_mgt = True
    # -------------------------------------------------------------------------

    try:
        # 1. Retrieve data for all assets (thread-safe)
        async with realtime_buffer_mutex:
            # Create a copy of the buffer to avoid modification during serialization
            response_data = realtime_data_buffer.copy()

        # 2. Send the data as a JSON response
        _logger.debug("Fetched real-time data for all assets")
        return web.json_response(response_data)

    except web.HTTPException: # Re-raise HTTP exceptions
        raise
    except Exception as ex:
        _logger.exception("Error fetching all real-time data: %s", str(ex))
        raise web.HTTPInternalServerError(reason=f"Internal error fetching data: {str(ex)}")
# --------------------------------------

# --- CORS Middleware ---
def cors_middleware_factory():
    """Factory to create a CORS middleware."""
    # Importing web here is generally fine for middleware factories
    from aiohttp import web
    @web.middleware
    async def cors_middleware(request, handler):
        """CORS middleware to add appropriate headers."""
        # --- Handle preflight OPTIONS requests ---
        if request.method == 'OPTIONS':
            headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization, If-Match, If-None-Match',
                'Access-Control-Max-Age': '86400',  # Cache preflight response for 24 hours (optional)
            }
            return web.Response(status=200, headers=headers)

        # --- Process the actual request ---
        try:
            response = await handler(request)
        except web.HTTPException as ex:
            # Still add CORS headers to error responses if needed
            if 'Access-Control-Allow-Origin' not in ex.headers:
                ex.headers['Access-Control-Allow-Origin'] = '*'
            if 'Access-Control-Allow-Methods' not in ex.headers:
                ex.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            raise ex # Re-raise the exception

        # --- Add CORS headers to successful responses ---
        if not response.headers.get('Access-Control-Allow-Origin'):
            response.headers['Access-Control-Allow-Origin'] = '*'
        if not response.headers.get('Access-Control-Allow-Methods'):
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
        if not response.headers.get('Access-Control-Allow-Headers'):
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, If-Match, If-None-Match'

        return response
    return cors_middleware
# ----------------------

# Optional: Function to get the buffer for debugging or other internal use
# Note: This synchronous function accessing an async lock is problematic.
# A correct implementation would need an async context:
# async def get_buffer_snapshot_async():
#     async with realtime_buffer_mutex:
#         return realtime_data_buffer.copy()
def get_buffer_snapshot():
    """Return a copy of the current data buffer for debugging."""
    # Placeholder for the problematic sync function
    # As-is, this won't work correctly due to the async lock.
    pass
