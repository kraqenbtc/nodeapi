import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from dotenv import load_dotenv

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

def get_db_connection():
    """Veritabanına bağlantı oluşturur ve döndürür"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            cursor_factory=RealDictCursor  # Dict formatında sonuçlar döndürür
        )
        conn.autocommit = True  # Otomatik commit
        return conn
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Veritabanı bağlantısı başarısız: {error}")
        return None

def execute_query(query, params=None):
    """
    SQL sorgusu yürütür ve sonuçları döndürür
    
    Args:
        query (str): Çalıştırılacak SQL sorgusu
        params (tuple, optional): SQL parametreleri
        
    Returns:
        list: Sorgu sonuçları (dict formatta)
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Veritabanı bağlantısı kurulamadı")
        return []
    
    cursor = conn.cursor()
    results = []
    
    try:
        cursor.execute(query, params)
        if cursor.description:  # Sorgu sonuç dönüyorsa
            results = cursor.fetchall()
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Sorgu hatası: {error}")
    finally:
        cursor.close()
        conn.close()
    
    return results 