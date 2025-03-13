import time
import logging
import hashlib
import json
from functools import wraps

# Önbellek için logger
logger = logging.getLogger("db-cache")

# Basit önbellek sözlüğü
cache = {}

# Önbellek ayarları
MAX_CACHE_SIZE = 1000  # Maksimum önbellek öğe sayısı
DEFAULT_TTL = 60       # Varsayılan TTL süresi (saniye)

def cache_key(query, params=None):
    """Sorgu ve parametrelerinden önbellek anahtarı oluşturur"""
    # Parametreler varsa, JSON string olarak dönüştür
    params_str = json.dumps(params, sort_keys=True) if params else "None"
    
    # Sorgu ve parametrelerden hash oluştur
    key = hashlib.md5(f"{query}:{params_str}".encode()).hexdigest()
    return key

def get_from_cache(key):
    """Önbellekten veri alır"""
    if key in cache:
        # Veri var, TTL kontrolü yap
        cached_data = cache[key]
        if time.time() < cached_data['expires_at']:
            # Veri hala geçerli
            logger.debug(f"Cache hit: {key[:8]}...")
            return cached_data['data']
        else:
            # TTL süresi dolmuş, veriyi sil
            logger.debug(f"Cache expired: {key[:8]}...")
            del cache[key]
    
    # Veri bulunamadı veya süresi dolmuş
    logger.debug(f"Cache miss: {key[:8]}...")
    return None

def set_in_cache(key, data, ttl=DEFAULT_TTL):
    """Veriyi önbelleğe kaydeder"""
    # Önbellek boyutu kontrolü
    if len(cache) >= MAX_CACHE_SIZE:
        # En eski 10% öğeyi temizle
        items_to_remove = sorted(
            cache.items(),
            key=lambda x: x[1]['expires_at']
        )[:int(MAX_CACHE_SIZE * 0.1)]
        
        for k, _ in items_to_remove:
            del cache[k]
        
        logger.debug(f"Cache cleaned: removed {len(items_to_remove)} items")
    
    # Veriyi önbelleğe ekle
    cache[key] = {
        'data': data,
        'expires_at': time.time() + ttl,
        'created_at': time.time()
    }
    logger.debug(f"Cache set: {key[:8]}...")
    return True

def cached_query(ttl=DEFAULT_TTL):
    """
    Sorgu sonuçlarını önbellekleyen bir dekoratör
    
    Args:
        ttl (int): Önbellek geçerlilik süresi (saniye)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(query, params=None, bypass_cache=False, *args, **kwargs):
            # Eğer önbellek devre dışı bırakıldıysa, doğrudan sorgu çalıştır
            if bypass_cache:
                return func(query, params, *args, **kwargs)
            
            # Önbellek anahtarı oluştur
            key = cache_key(query, params)
            
            # Önbellekten veri almayı dene
            cached_result = get_from_cache(key)
            if cached_result is not None:
                return cached_result
            
            # Önbellekte yoksa, sorguyu çalıştır
            result = func(query, params, *args, **kwargs)
            
            # Sonucu önbelleğe ekle
            set_in_cache(key, result, ttl)
            
            return result
        return wrapper
    return decorator

def clear_cache():
    """Tüm önbelleği temizler"""
    global cache
    cache = {}
    logger.info("Cache cleared")

def get_cache_stats():
    """Önbellek istatistiklerini döndürür"""
    return {
        'size': len(cache),
        'max_size': MAX_CACHE_SIZE,
        'items': [
            {
                'key': k[:8] + "...",
                'expires_in': int(v['expires_at'] - time.time()),
                'age': int(time.time() - v['created_at'])
            }
            for k, v in sorted(
                cache.items(),
                key=lambda x: x[1]['expires_at'],
                reverse=True
            )[:10]  # Sadece ilk 10 öğeyi göster
        ]
    } 