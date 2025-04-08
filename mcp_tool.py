from mcp.server.fastmcp import FastMCP
from mcp_server import DatabaseServer
import json
import os
import sys
import glob
import contextlib
import re
from typing import Optional, Dict, Any, List, Union

# Đặt mã hóa UTF-8 cho đầu ra
sys.stdout.reconfigure(encoding='utf-8')

# Khởi tạo FastMCP
mcp = FastMCP("DatabaseTool")

# Danh sách các database đã phát hiện
available_databases = {}

def discover_databases():
    """Tự động phát hiện các database có sẵn trong thư mục hiện tại"""
    databases = {}
    
    # Tìm các file SQLite
    for db_file in glob.glob("*.db"):
        db_name = os.path.splitext(os.path.basename(db_file))[0]
        databases[db_name] = {
            "type": "sqlite",
            "path": db_file
        }
        print(f"[DISCOVER] Tìm thấy SQLite database: {db_name}")
    
    # Đọc cấu hình MySQL từ file (nếu có)
    if os.path.exists("mysql_config.json"):
        try:
            with open("mysql_config.json", "r") as f:
                mysql_configs = json.load(f)
            
            for config in mysql_configs:
                if "name" in config and "database" in config:
                    db_name = config.get("name")
                    databases[db_name] = {
                        "type": "mysql",
                        "host": config.get("host", "localhost"),
                        "user": config.get("user", "root"),
                        "password": config.get("password", ""),
                        "database": config.get("database"),
                        "port": config.get("port", 3306)
                    }
                    print(f"[DISCOVER] Tìm thấy MySQL database: {db_name} ({config.get('database')})")
        except Exception as e:
            print(f"[ERROR] Lỗi khi đọc cấu hình MySQL: {str(e)}")
    
    return databases

class DatabaseHelper:
    """Helper class để làm việc với database"""
    
    @staticmethod
    @contextlib.contextmanager
    def connect_to_database(db_name):
        """Kết nối tới database bằng tên"""
        if db_name not in available_databases:
            raise Exception(f"Không tìm thấy database: {db_name}")
        
        db_config = available_databases[db_name]
        server = DatabaseServer()
        
        # Kết nối dựa trên loại database
        if db_config["type"] == "sqlite":
            result = server.connect_sqlite(db_config["path"])
        elif db_config["type"] == "mysql":
            result = server.connect_mysql(
                db_config["host"],
                db_config["user"],
                db_config["password"],
                db_config["database"],
                db_config["port"]
            )
        else:
            raise Exception(f"Loại database không hỗ trợ: {db_config['type']}")
        
        if result.get("status") != "success":
            raise Exception(f"Không thể kết nối đến database: {result.get('message')}")
        
        try:
            yield server
        finally:
            server.disconnect()
    
    @staticmethod
    def is_safe_query(query):
        """Kiểm tra câu lệnh SQL có an toàn không (chỉ SELECT)"""
        # Chuẩn hóa câu lệnh
        query = query.strip().lower()
        
        # Chỉ cho phép câu lệnh bắt đầu bằng SELECT
        if not query.startswith("select "):
            return False
        
        # Kiểm tra từ khóa nguy hiểm
        dangerous_keywords = ["insert", "update", "delete", "drop", "alter", "create", "truncate"]
        for keyword in dangerous_keywords:
            # Kiểm tra nếu từ khóa xuất hiện như một từ hoàn chỉnh
            pattern = r'\b' + keyword + r'\b'
            if re.search(pattern, query):
                return False
        
        return True

@mcp.tool()
def list_available_databases() -> str:
    """Liệt kê tất cả các database có sẵn"""
    global available_databases
    
    if not available_databases:
        return "[INFO] Không tìm thấy database nào."
    
    result = {"databases": {}}
    
    for name, config in available_databases.items():
        db_info = {
            "type": config["type"]
        }
        
        if config["type"] == "sqlite":
            db_info["path"] = config["path"]
        elif config["type"] == "mysql":
            db_info["host"] = config["host"]
            db_info["database"] = config["database"]
            db_info["user"] = config["user"]
        
        result["databases"][name] = db_info
    
    return json.dumps(result, indent=2)

@mcp.tool()
def explore_database(db_name: str, action: str = "list_tables", table_name: str = None, limit: int = 100, search_term: str = None) -> str:
    """
    Khám phá database và dữ liệu
    
    Args:
        db_name: Tên database để khám phá
        action: Hành động cần thực hiện (list_tables, describe_table, get_data, search_data)
        table_name: Tên bảng (cần thiết cho describe_table, get_data, search_data)
        limit: Số lượng bản ghi tối đa trả về (cho get_data, search_data)
        search_term: Từ khóa tìm kiếm (cho search_data)
    
    Returns:
        Kết quả truy vấn
    """
    try:
        with DatabaseHelper.connect_to_database(db_name) as server:
            if action == "list_tables":
                result = server.get_table_names()
                if not result:
                    return f"[INFO] Database {db_name} không có bảng nào."
                return json.dumps({"database": db_name, "tables": result}, indent=2)
            
            elif action == "describe_table":
                if not table_name:
                    return "[ERROR] Vui lòng cung cấp tham số table_name."
                
                result = server.get_table_schema(table_name)
                if not result:
                    return f"[INFO] Không tìm thấy bảng: {table_name} trong database {db_name}."
                return result
            
            elif action == "get_data":
                if not table_name:
                    return "[ERROR] Vui lòng cung cấp tham số table_name."
                
                result = server.get_all_data(table_name, limit)
                if not result:
                    return f"[INFO] Không có dữ liệu trong bảng: {table_name} của database {db_name}."
                return result
            
            elif action == "search_data":
                if not table_name:
                    return "[ERROR] Vui lòng cung cấp tham số table_name."
                if not search_term:
                    return "[ERROR] Vui lòng cung cấp tham số search_term."
                
                result = server.search_data(table_name, search_term, limit=limit)
                if not result:
                    return f"[INFO] Không tìm thấy dữ liệu phù hợp với từ khóa '{search_term}' trong bảng: {table_name}."
                return result
            
            else:
                return f"[ERROR] Hành động không hợp lệ: {action}. Các hành động hợp lệ: list_tables, describe_table, get_data, search_data"
    
    except Exception as e:
        return f"[ERROR] {str(e)}"

@mcp.tool()
def execute_query(db_name: str, query: str) -> str:
    """
    Thực thi câu lệnh SQL chỉ đọc (SELECT) trên database
    
    Args:
        db_name: Tên database để thực thi câu lệnh
        query: Câu lệnh SQL (chỉ cho phép SELECT)
    
    Returns:
        Kết quả của câu lệnh SQL
    """
    # Kiểm tra câu lệnh SQL có an toàn không
    if not DatabaseHelper.is_safe_query(query):
        return "[ERROR] Chỉ cho phép câu lệnh SELECT để đảm bảo chế độ chỉ đọc"
    
    try:
        with DatabaseHelper.connect_to_database(db_name) as server:
            return server.execute_query(query)
    except Exception as e:
        return f"[ERROR] {str(e)}"

@mcp.tool()
def get_database_summary(db_name: str) -> str:
    """
    Lấy thông tin tổng quan về database
    
    Args:
        db_name: Tên database để lấy thông tin
    
    Returns:
        Thông tin tổng quan về database
    """
    try:
        with DatabaseHelper.connect_to_database(db_name) as server:
            result = server.get_database_info()
            if not result:
                return f"[INFO] Không có thông tin nào về database {db_name}."
            return result
    except Exception as e:
        return f"[ERROR] {str(e)}"

@mcp.tool()
def rescan_databases() -> str:
    """Quét lại tất cả các database có sẵn"""
    global available_databases
    
    old_count = len(available_databases)
    available_databases = discover_databases()
    new_count = len(available_databases)
    
    if new_count == 0:
        return "[INFO] Không tìm thấy database nào."
    elif new_count > old_count:
        return f"[SUCCESS] Đã phát hiện {new_count} database, thêm {new_count - old_count} database mới."
    elif new_count < old_count:
        return f"[INFO] Đã phát hiện {new_count} database, giảm {old_count - new_count} database so với lần quét trước."
    else:
        return f"[INFO] Không có thay đổi, vẫn có {new_count} database có sẵn."

@mcp.tool()
def add_mysql_database(name: str, host: str, user: str, password: str, database: str, port: int = 3306) -> str:
    """
    Thêm cấu hình MySQL database mới
    
    Args:
        name: Tên để tham chiếu đến database
        host: Địa chỉ host MySQL
        user: Tên người dùng
        password: Mật khẩu
        database: Tên database
        port: Cổng kết nối (mặc định: 3306)
    
    Returns:
        Kết quả của việc thêm cấu hình
    """
    global available_databases
    
    # Kiểm tra xem tên đã tồn tại chưa
    if name in available_databases:
        return f"[ERROR] Database với tên '{name}' đã tồn tại."
    
    # Kiểm tra kết nối
    server = DatabaseServer()
    result = server.connect_mysql(host, user, password, database, port)
    
    if result.get("status") != "success":
        server.disconnect()
        return f"[ERROR] Không thể kết nối đến MySQL database: {result.get('message')}"
    
    server.disconnect()
    
    # Lưu cấu hình
    available_databases[name] = {
        "type": "mysql",
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port
    }
    
    # Lưu vào file cấu hình
    try:
        configs = []
        for db_name, config in available_databases.items():
            if config["type"] == "mysql":
                configs.append({
                    "name": db_name,
                    "host": config["host"],
                    "user": config["user"],
                    "password": config["password"],
                    "database": config["database"],
                    "port": config["port"]
                })
        
        with open("mysql_config.json", "w") as f:
            json.dump(configs, f, indent=2)
        
        return f"[SUCCESS] Đã thêm MySQL database '{name}' và lưu cấu hình."
    except Exception as e:
        # Xóa khỏi available_databases nếu lưu file thất bại
        del available_databases[name]
        return f"[ERROR] Lỗi khi lưu cấu hình: {str(e)}"

class DatabaseChatClient:
    """
    Client tương tác với người dùng qua giao diện chat
    """
    def __init__(self):
        self.last_input = ""
    
    def handle_input(self, user_input):
        """Xử lý đầu vào từ người dùng để tránh lỗi HumanMessage empty content"""
        if not user_input.strip():
            # Nếu đầu vào trống, sử dụng lại đầu vào cuối cùng hoặc trả về một thông báo
            if self.last_input:
                return self.last_input
            else:
                return "help"  # Mặc định trả về "help" nếu người dùng không nhập gì
        
        self.last_input = user_input
        return user_input
    
    def handle_response(self, response):
        """Xử lý phản hồi trước khi hiển thị cho người dùng"""
        # Có thể thêm xử lý phản hồi ở đây nếu cần
        return response
    
    def start(self):
        """Khởi động client chat"""
        print("=== Chào mừng đến với Database Chat Agent ===")
        print("Nhập 'exit' hoặc 'quit' để thoát")
        
        # Khởi tạo LangChain và các thành phần khác ở đây
        # ...
        
        while True:
            try:
                user_input = input("Bạn: ")
                
                if user_input.lower() in ["exit", "quit"]:
                    print("Tạm biệt!")
                    break
                
                # Xử lý đầu vào để tránh lỗi empty content
                processed_input = self.handle_input(user_input)
                
                # Xử lý câu hỏi và lấy phản hồi
                # response = langchain_agent.run(processed_input)
                response = "Giả lập phản hồi từ agent"  # Thay thế dòng này bằng mã thực tế
                
                # Xử lý phản hồi trước khi hiển thị
                processed_response = self.handle_response(response)
                
                print(f"Agent: {processed_response}")
                
            except KeyboardInterrupt:
                print("\nTạm biệt!")
                break
            except Exception as e:
                print(f"Lỗi: {str(e)}")

if __name__ == "__main__":
    print("=== Đang quét database có sẵn... ===")
    available_databases = discover_databases()
    print(f"=== Đã phát hiện {len(available_databases)} database ===")
    
    print("=== Khởi động FastMCP Server ===")
    mcp.run(transport="stdio")
    
    # Nếu muốn chạy giao diện chat thay vì FastMCP
    # client = DatabaseChatClient()
    # client.start()