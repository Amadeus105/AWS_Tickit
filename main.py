import psycopg2
import pandas as pd
from datetime import datetime
import os

class DatabaseAnalyzer:
    def __init__(self):
        self.connection = None
        self.connect()
    
    def connect(self):
        """Подключение к базе данных"""
        try:
            self.connection = psycopg2.connect(
                host="localhost",
                database="AWS_Tickit_Database",  # замените на имя вашей БД
                user="postgres",          # замените на ваше имя пользователя
                password="0000",      # замените на ваш пароль
                port="5432"
            )
            print("✅ Успешно подключились к базе данных")
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
            raise
    
    def execute_query(self, query, description=None):
        """Выполнение SQL-запроса и вывод результатов"""
        try:
            with self.connection.cursor() as cursor:
                if description:
                    print(f"\n{'='*60}")
                    print(f"📊 {description}")
                    print(f"{'='*60}")
                
                cursor.execute(query)
                
                # Получаем результаты
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Выводим в терминал
                df = pd.DataFrame(results, columns=columns)
                print(f"📋 Результатов: {len(df)}")
                print(df.head(10))
                
                # Сохраняем в CSV
                if len(df) > 0:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"results/query_result_{timestamp}.csv"
                    os.makedirs("results", exist_ok=True)
                    df.to_csv(filename, index=False, encoding='utf-8')
                    print(f"💾 Результаты сохранены в: {filename}")
                
                return df
                
        except Exception as e:
            print(f"❌ Ошибка выполнения запроса: {e}")
            return None
    
    def run_analysis(self):
        """Запуск всех аналитических запросов"""
        queries = [
            {
                "query": "SELECT * FROM users LIMIT 10;",
                "description": "Первые 10 пользователей"
            },
            {
                "query": "SELECT * FROM events LIMIT 10;",
                "description": "Первые 10 событий"
            },
            {
                "query": """
                SELECT city, state, COUNT(*) as user_count
                FROM users 
                GROUP BY city, state 
                ORDER BY user_count DESC
                LIMIT 10;
                """,
                "description": "Топ-10 городов по количеству пользователей"
            },
            {
                "query": """
                SELECT e.eventid, e.eventname, e.starttime,
                       v.venuename, v.venuecity, v.venuestate
                FROM events e
                JOIN venue v ON e.venueid = v.venueid
                LIMIT 10;
                """,
                "description": "События с информацией о местах проведения"
            },
            {
                "query": """
                SELECT c.catgroup, c.catname,
                       COUNT(s.saleid) as total_sales,
                       SUM(s.qtysold) as total_tickets,
                       SUM(s.pricepaid) as total_revenue
                FROM sales s
                JOIN events e ON s.eventid = e.eventid
                JOIN category c ON e.catid = c.catid
                GROUP BY c.catgroup, c.catname
                ORDER BY total_revenue DESC;
                """,
                "description": "Рейтинг категорий по выручке"
            },
            {
                "query": """
                SELECT EXTRACT(YEAR FROM s.saletime) as year,
                       EXTRACT(MONTH FROM s.saletime) as month,
                       COUNT(s.saleid) as total_sales,
                       SUM(s.pricepaid) as total_revenue
                FROM sales s
                GROUP BY year, month
                ORDER BY year, month;
                """,
                "description": "Динамика продаж по месяцам"
            },
            {
                "query": """
                SELECT u.city, u.state,
                       COUNT(s.saleid) as total_purchases,
                       SUM(s.pricepaid) as total_revenue,
                       AVG(s.pricepaid) as avg_check
                FROM users u
                JOIN sales s ON u.userid = s.buyerid
                GROUP BY u.city, u.state
                HAVING COUNT(s.saleid) > 5
                ORDER BY avg_check DESC;
                """,
                "description": "Средний чек по городам"
            },
            {
                "query": """
                SELECT v.venuename, v.venuecity,
                       COUNT(DISTINCT e.eventid) as total_events,
                       SUM(s.pricepaid) as total_revenue
                FROM venue v
                JOIN events e ON v.venueid = e.venueid
                JOIN sales s ON e.eventid = s.eventid
                GROUP BY v.venueid, v.venuename, v.venuecity
                ORDER BY total_revenue DESC
                LIMIT 10;
                """,
                "description": "Топ-10 самых прибыльных мест"
            },
            {
                "query": """
                SELECT u.userid, u.username,
                       COUNT(s.saleid) as total_purchases,
                       SUM(s.pricepaid) as total_spent
                FROM users u
                JOIN sales s ON u.userid = s.buyerid
                GROUP BY u.userid, u.username
                ORDER BY total_spent DESC
                LIMIT 10;
                """,
                "description": "Топ-10 самых активных покупателей"
            },
            {
                "query": """
                SELECT EXTRACT(HOUR FROM s.saletime) as hour_of_day,
                       COUNT(s.saleid) as total_sales,
                       AVG(s.pricepaid) as avg_sale_amount
                FROM sales s
                GROUP BY EXTRACT(HOUR FROM s.saletime)
                ORDER BY hour_of_day;
                """,
                "description": "Продажи по часам дня"
            }
        ]
        
        print("🚀 Запуск анализа базы данных...")
        print(f"📅 Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Всего запросов: {len(queries)}")
        print("-" * 60)
        
        for i, query_info in enumerate(queries, 1):
            print(f"\n🔹 Запрос {i}/{len(queries)}")
            self.execute_query(query_info["query"], query_info["description"])
    
    def close(self):
        """Закрытие соединения с базой данных"""
        if self.connection:
            self.connection.close()
            print("✅ Соединение с базой данных закрыто")

def main():
    """Основная функция"""
    analyzer = None
    try:
        analyzer = DatabaseAnalyzer()
        analyzer.run_analysis()
    except Exception as e:
        print(f"❌ Произошла ошибка: {e}")
    finally:
        if analyzer:
            analyzer.close()

if __name__ == "__main__":
    main()