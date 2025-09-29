# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge-iot.readthedocs.io/
# FLEDGE_END

"""Real-time data handler for Fledge core API (port 8081)"""

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
async def south_data_post(request):
    """Handle POST data from South plugins to /south-data/{asset}"""
    # --- Mark request as Core Management related to bypass standard auth on main API ---
    # Setting this attribute tells the main API auth middleware to treat this specially.
    # This is a common Fledge pattern for internal/public endpoints on the main API.
    # MUST be set BEFORE the try block to ensure it's always applied.
    request.is_core_mgt = True
    # -------------------------------------------------------------------------

    try:
        # 1. Extract asset name from the URL path
        asset_name = request.match_info.get('asset', None)
        if not asset_name:
            raise web.HTTPBadRequest(reason="Asset name is required in the URL path.")

        # 2. Parse JSON data from the request body
        try:
            data = await request.json()
        except json.JSONDecodeError:
            raise web.HTTPBadRequest(reason="Invalid JSON data in request body.")

        # 3. Prepare the data entry with a timestamp
        data_entry = {
            "received_at": datetime.now().isoformat(), # Add timestamp
            "data": data  # Store the original data sent by the plugin
        }

        # 4. Store the data entry in the buffer (thread-safe)
        async with realtime_buffer_mutex:
            # Initialize the list for this asset if it doesn't exist
            if asset_name not in realtime_data_buffer:
                realtime_data_buffer[asset_name] = []

            # Append the new data entry
            realtime_data_buffer[asset_name].append(data_entry)

            # 5. Maintain buffer size limit
            if len(realtime_data_buffer[asset_name]) > MAX_BUFFER_SIZE:
                # Keep only the last MAX_BUFFER_SIZE entries
                realtime_data_buffer[asset_name] = \
                    realtime_data_buffer[asset_name][-MAX_BUFFER_SIZE:]

        # 6. Prepare and send successful response
        response_data = {
            "status": "success",
            "message": f"Data received for asset '{asset_name}'",
            "asset": asset_name
        }
        _logger.info(f"Received data via POST for asset: {asset_name}")
        return web.json_response(response_data)

    except web.HTTPException: # Re-raise HTTP exceptions (e.g., 400 Bad Request)
        raise
    except Exception as ex:
        _logger.exception("Error handling south data POST for asset %s: %s", asset_name if 'asset_name' in locals() else 'unknown', str(ex))
        raise web.HTTPInternalServerError(reason=f"Internal error processing data: {str(ex)}")


async def get_realtime_data(request):
    """Handle GET requests to /api/realtime/{asset}"""
    # --- Mark request as Core Management related to bypass standard auth on main API ---
    # Setting this attribute makes this endpoint publicly accessible on the main API port.
    # MUST be set BEFORE the try block to ensure it's always applied.
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
    # MUST be set BEFORE the try block to ensure it's always applied.
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

# --- CORS Middleware (Optional, if not using server.py middleware) ---
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
    # Consider using get_buffer_snapshot_async() instead.
    pass # Or return a copy if you implement a thread-safe mechanism elsewhere
# --------------------------------------
