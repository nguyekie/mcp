import sqlite3
import mysql.connector
from mysql.connector import Error as MySQLError
import json

class DatabaseServer:
    """Server để quản lý kết nối và thao tác với cơ sở dữ liệu"""
    
    def __init__(self):
        self.connection = None
        self.db_name = None
        self.db_type = None
        self.read_only = False
    
    def connect_sqlite(self, db_path, read_only=False):
        """Kết nối tới SQLite database với tùy chọn chế độ chỉ đọc"""
        try:
            if read_only:
                # Kết nối với chế độ chỉ đọc bằng URI
                self.connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
                self.read_only = True
            else:
                self.connection = sqlite3.connect(db_path)
                self.read_only = False
                
            self.connection.row_factory = sqlite3.Row
            self.db_name = db_path
            self.db_type = "SQLite"
            return {"status": "success", "message": f"Đã kết nối tới SQLite database: {db_path}" + (" (CHỈ ĐỌC)" if read_only else "")}
        except sqlite3.Error as e:
            return {"status": "error", "message": f"Lỗi khi kết nối tới SQLite database: {str(e)}"}
    
    def connect_mysql(self, host, user, password, database, port=3306, read_only=False):
        """Kết nối tới MySQL database với tùy chọn chế độ chỉ đọc"""
        try:
            self.connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port
            )
            self.db_name = database
            self.db_type = "MySQL"
            self.read_only = read_only
            
            # Nếu là chế độ chỉ đọc, đặt session thành read-only nếu có thể
            if read_only:
                cursor = self.connection.cursor()
                try:
                    # Thiết lập session thành READ ONLY cho MySQL
                    cursor.execute("SET SESSION TRANSACTION READ ONLY")
                    cursor.close()
                except:
                    # Nếu không hỗ trợ, vẫn tiếp tục nhưng cảnh báo
                    cursor.close()
                    print("CẢNH BÁO: Không thể thiết lập chế độ READ ONLY cho MySQL")
            
            return {"status": "success", "message": f"Đã kết nối tới MySQL database: {database} tại {host}:{port}" + (" (CHỈ ĐỌC)" if read_only else "")}
        except MySQLError as e:
            return {"status": "error", "message": f"Lỗi khi kết nối tới MySQL database: {str(e)}"}
    
    def disconnect(self):
        """Đóng kết nối database"""
        if self.connection:
            self.connection.close()
            self.connection = None
            db_name = self.db_name
            self.db_name = None
            self.db_type = None
            self.read_only = False
            return {"status": "success", "message": f"Đã đóng kết nối tới database: {db_name}"}
        return {"status": "error", "message": "Không có kết nối database nào để đóng"}
    
    def get_table_names(self):
        """Lấy danh sách tên các bảng trong database"""
        if not self.connection:
            return []
        
        cursor = self.connection.cursor()
        if self.db_type == "SQLite":
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']
        elif self.db_type == "MySQL":
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
        else:
            tables = []
        
        cursor.close()
        return tables
    
    def get_table_schema(self, table_name):
        """Lấy schema của bảng"""
        if not self.connection:
            return {"status": "error", "message": "Không có kết nối database"}
        
        try:
            cursor = self.connection.cursor()
            if self.db_type == "SQLite":
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = []
                for row in cursor.fetchall():
                    columns.append({
                        "name": row[1],
                        "type": row[2],
                        "not_null": bool(row[3]),
                        "default_value": row[4],
                        "primary_key": bool(row[5])
                    })
            elif self.db_type == "MySQL":
                cursor.execute(f"DESCRIBE {table_name}")
                columns = []
                for row in cursor.fetchall():
                    columns.append({
                        "name": row[0],
                        "type": row[1],
                        "not_null": row[2] == "NO",
                        "key": row[3],
                        "default_value": row[4],
                        "extra": row[5]
                    })
            else:
                return {"status": "error", "message": "Loại database không được hỗ trợ"}
            
            cursor.close()
            return {"status": "success", "table": table_name, "schema": columns}
        except Exception as e:
            return {"status": "error", "message": f"Lỗi khi lấy schema của bảng {table_name}: {str(e)}"}
    
    def get_all_data(self, table_name, limit=100):
        """Lấy toàn bộ dữ liệu từ bảng"""
        if not self.connection:
            return {"status": "error", "message": "Không có kết nối database"}
        
        try:
            cursor = self.connection.cursor()
            # Lấy tên cột
            if self.db_type == "SQLite":
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
            elif self.db_type == "MySQL":
                cursor.execute(f"SHOW COLUMNS FROM {table_name}")
                columns = [row[0] for row in cursor.fetchall()]
            else:
                return {"status": "error", "message": "Loại database không được hỗ trợ"}
            
            # Đếm tổng số bản ghi
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            
            # Lấy dữ liệu với giới hạn
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            rows = cursor.fetchall()
            
            # Chuyển đổi dữ liệu sang dictionary
            data = []
            if self.db_type == "SQLite":
                for row in rows:
                    data.append({columns[i]: row[i] for i in range(len(columns))})
            elif self.db_type == "MySQL":
                for row in rows:
                    data.append({columns[i]: row[i] for i in range(len(columns))})
            
            cursor.close()
            return {
                "status": "success", 
                "data": data, 
                "count": len(data),
                "total": total_count,
                "limited": total_count > limit
            }
        except Exception as e:
            return {"status": "error", "message": f"Lỗi khi lấy dữ liệu từ bảng {table_name}: {str(e)}"}
    
    def execute_query(self, query, params=None):
        """Thực thi câu lệnh SQL"""
        if not self.connection:
            return {"status": "error", "message": "Không có kết nối database"}
        
        try:
            # Nếu là chế độ chỉ đọc, ngăn chặn các câu lệnh ghi dữ liệu
            if self.read_only:
                query_lower = query.strip().lower()
                if (query_lower.startswith('insert') or 
                    query_lower.startswith('update') or 
                    query_lower.startswith('delete') or 
                    query_lower.startswith('drop') or 
                    query_lower.startswith('alter') or 
                    query_lower.startswith('create')):
                    return {"status": "error", "message": "Không thể thực hiện lệnh ghi dữ liệu ở chế độ CHỈ ĐỌC"}
            
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Nếu là câu lệnh SELECT
            if query.strip().upper().startswith(("SELECT", "SHOW", "PRAGMA", "EXPLAIN", "DESCRIBE", "DESC")):
                # Lấy tên cột
                column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Lấy kết quả
                rows = cursor.fetchall()
                result_data = []
                
                if self.db_type == "SQLite":
                    for row in rows:
                        result_data.append({column_names[i]: row[i] for i in range(len(column_names))})
                elif self.db_type == "MySQL":
                    for row in rows:
                        result_data.append({column_names[i]: row[i] for i in range(len(column_names))})
                
                self.connection.commit()
                cursor.close()
                return {"status": "success", "data": result_data, "count": len(result_data)}
            else:
                # Nếu là câu lệnh INSERT, UPDATE, DELETE
                affected_rows = cursor.rowcount
                self.connection.commit()
                cursor.close()
                return {"status": "success", "affected_rows": affected_rows, "message": "Thực thi thành công"}
        except Exception as e:
            return {"status": "error", "message": f"Lỗi khi thực thi câu lệnh SQL: {str(e)}"}
    
    def get_database_info(self):
        """Lấy thông tin tổng quan về database"""
        if not self.connection:
            return {"status": "error", "message": "Không có kết nối database"}
        
        try:
            tables = self.get_table_names()
            table_info = []
            
            for table_name in tables:
                schema_result = self.get_table_schema(table_name)
                if schema_result["status"] == "success":
                    count_query = f"SELECT COUNT(*) FROM {table_name}"
                    cursor = self.connection.cursor()
                    cursor.execute(count_query)
                    row_count = cursor.fetchone()[0]
                    cursor.close()
                    
                    table_info.append({
                        "name": table_name,
                        "columns": len(schema_result["schema"]),
                        "rows": row_count
                    })
            
            info = {
                "type": self.db_type,
                "name": self.db_name,
                "tables": len(tables),
                "table_details": table_info,
                "read_only": self.read_only
            }
            
            return {"status": "success", "info": info}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def search_data(self, table_name, search_term, columns=None, limit=100):
        """Tìm kiếm dữ liệu trong bảng"""
        if not self.connection:
            return {"status": "error", "message": "Không có kết nối database"}
        
        try:
            # Lấy tất cả các cột nếu không chỉ định
            if not columns:
                schema_result = self.get_table_schema(table_name)
                if schema_result["status"] != "success":
                    return {"status": "error", "message": "Không thể lấy thông tin bảng"}
                columns = [col["name"] for col in schema_result["schema"]]
            
            # Xây dựng câu truy vấn tìm kiếm
            placeholders = " OR ".join([f"{col} LIKE ?" for col in columns])
            query = f"SELECT * FROM {table_name} WHERE {placeholders} LIMIT {limit}"
            
            # Chuẩn bị tham số
            params = [f"%{search_term}%" for _ in columns]
            
            # Thực thi truy vấn
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            
            # Lấy tên cột
            column_names = [desc[0] for desc in cursor.description]
            
            # Lấy kết quả
            rows = cursor.fetchall()
            result_data = []
            
            if self.db_type == "SQLite":
                for row in rows:
                    result_data.append({column_names[i]: row[i] for i in range(len(column_names))})
            elif self.db_type == "MySQL":
                for row in rows:
                    result_data.append({column_names[i]: row[i] for i in range(len(column_names))})
            
            cursor.close()
            return {"status": "success", "data": result_data, "count": len(result_data)}
        except Exception as e:
            return {"status": "error", "message": f"Lỗi khi tìm kiếm dữ liệu: {str(e)}"}