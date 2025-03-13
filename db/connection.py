import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging
from dotenv import load_dotenv
import time

# .env dosyasını yükle
load_dotenv()

# Logger yapılandırması
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("db-connection")

# Veritabanı bağlantı parametreleri
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'kraxeldb')
DB_USER = os.getenv('DB_USER', 'kraxeluser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')

# Connection pool yapılandırması
MIN_CONNECTIONS = 5  # Minimum bağlantı sayısı
MAX_CONNECTIONS = 20  # Maksimum bağlantı sayısı
CONNECTION_TIMEOUT = 5  # Bağlantı timeout süresi (saniye)

# Bağlantı havuzu
connection_pool = None

# Cache modülünü içe aktar
from db.cache import cached_query

def init_connection_pool():
    """Veritabanı bağlantı havuzunu başlatır"""
    global connection_pool
    
    try:
        connection_pool = pool.ThreadedConnectionPool(
            MIN_CONNECTIONS,
            MAX_CONNECTIONS,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logger.info(f"Veritabanı bağlantı havuzu başlatıldı (min: {MIN_CONNECTIONS}, max: {MAX_CONNECTIONS})")
        return True
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Bağlantı havuzu oluşturma hatası: {error}")
        return False

# Uygulama başlatıldığında bağlantı havuzunu oluştur
init_connection_pool()

def get_db_connection():
    """Bağlantı havuzundan bir bağlantı alır"""
    global connection_pool
    
    # Eğer bağlantı havuzu yoksa, yeniden oluşturmayı dene
    if connection_pool is None:
        if not init_connection_pool():
            logger.error("Bağlantı havuzu oluşturulamadı")
            return None
    
    # Havuzdan bağlantı almayı birkaç kez dene
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            connection = connection_pool.getconn()
            if connection:
                return connection
        except (Exception, psycopg2.pool.PoolError) as error:
            logger.error(f"Havuzdan bağlantı alınamadı (deneme {retry_count+1}/{max_retries}): {error}")
            retry_count += 1
            time.sleep(0.5)  # Kısa bir süre bekle
    
    # Son çare olarak doğrudan bağlantı oluştur
    logger.warning("Bağlantı havuzu başarısız, doğrudan bağlantı oluşturuluyor")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=CONNECTION_TIMEOUT
        )
        conn.autocommit = True
        return conn
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Doğrudan veritabanı bağlantısı başarısız: {error}")
        return None

def release_connection(conn):
    """Bağlantıyı havuza geri döndürür"""
    global connection_pool
    
    if connection_pool is not None and conn is not None:
        try:
            connection_pool.putconn(conn)
            return True
        except (Exception, psycopg2.pool.PoolError) as error:
            logger.error(f"Bağlantı havuza döndürülemedi: {error}")
    
    # Eğer havuz yoksa veya bağlantı döndürülemezse, bağlantıyı kapat
    if conn is not None:
        try:
            conn.close()
        except:
            pass
    
    return False

@cached_query(ttl=60)  # Sonuçları 60 saniyeliğine önbellekle
def execute_query(query, params=None, bypass_cache=False):
    """
    SQL sorgusu yürütür ve sonuçları döndürür
    
    Args:
        query (str): Çalıştırılacak SQL sorgusu
        params (tuple, optional): SQL parametreleri
        bypass_cache (bool, optional): Önbelleği atlayıp doğrudan sorgu çalıştırır
        
    Returns:
        list: Sorgu sonuçları (dict formatta)
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Veritabanı bağlantısı kurulamadı")
        return []
    
    cursor = None
    results = []
    
    try:
        # Dict formatında sonuçlar döndürmek için RealDictCursor kullan
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Sorgu timeout süresi ayarla
        cursor.execute("SET statement_timeout = 30000;")  # 30 saniye
        
        # Sorguyu çalıştır
        cursor.execute(query, params)
        
        # Sonuçları al
        if cursor.description:  # Sorgu sonuç dönüyorsa
            results = cursor.fetchall()
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Sorgu hatası: {error}")
        if isinstance(error, psycopg2.extensions.QueryCanceledError):
            logger.error("Sorgu zaman aşımına uğradı - sorgu optimize edilmeli")
    finally:
        # Cursor'ı kapat
        if cursor:
            cursor.close()
        
        # Bağlantıyı havuza geri döndür veya kapat
        release_connection(conn)
    
    return results 