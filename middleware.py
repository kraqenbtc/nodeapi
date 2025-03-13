import time
import logging
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

# Logger yapılandırması
logger = logging.getLogger("api-middleware")

class PerformanceMiddleware(BaseHTTPMiddleware):
    """
    API performansını izleyen middleware
    
    Bu middleware API isteklerinin işlenme süresini ölçer
    ve yavaş istekleri loglar.
    """
    
    def __init__(self, app, slow_threshold_ms=500):
        super().__init__(app)
        self.slow_threshold_ms = slow_threshold_ms
    
    async def dispatch(self, request: Request, call_next):
        # İstek başlangıç zamanı
        start_time = time.time()
        
        # İstek bilgilerini al
        client_ip = request.client.host if request.client else "unknown"
        path = request.url.path
        query_params = str(request.query_params)
        
        # İsteği işle
        response = await call_next(request)
        
        # Geçen süreyi hesapla
        process_time_ms = (time.time() - start_time) * 1000
        
        # Yavaş istekleri logla
        if process_time_ms > self.slow_threshold_ms:
            logger.warning(
                f"Slow request detected: {request.method} {path} - "
                f"{process_time_ms:.2f}ms (threshold: {self.slow_threshold_ms}ms) - "
                f"Client: {client_ip}, Query: {query_params}"
            )
        
        # Tüm isteklerin işlenme süresini response header'a ekle
        response.headers["X-Process-Time-Ms"] = f"{process_time_ms:.2f}"
        
        # Normal istekler için debug log
        logger.debug(
            f"Request: {request.method} {path} - "
            f"{process_time_ms:.2f}ms - Client: {client_ip}"
        )
        
        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    API isteklerini loglayan middleware
    """
    
    async def dispatch(self, request: Request, call_next):
        # İstek başlangıç zamanı
        start_time = time.time()
        
        # İstek bilgilerini al
        client_ip = request.client.host if request.client else "unknown"
        path = request.url.path
        method = request.method
        
        # İstek başlangıcını logla
        logger.info(f"Request started: {method} {path} - Client: {client_ip}")
        
        # İsteği işle ve response'u al
        try:
            response = await call_next(request)
            
            # İşlenme süresini hesapla
            process_time_ms = (time.time() - start_time) * 1000
            
            # Başarılı isteği logla
            logger.info(
                f"Request completed: {method} {path} - "
                f"Status: {response.status_code} - {process_time_ms:.2f}ms"
            )
            
            return response
        except Exception as e:
            # Hatayı logla
            logger.error(
                f"Request failed: {method} {path} - "
                f"Error: {str(e)}"
            )
            raise 