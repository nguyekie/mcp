from mcp.server.fastmcp import FastMCP
from mcp_server import DatabaseServer
import json
import os
import sys
import glob
import re
from typing import Optional, Dict, Any, List, Union
from tabulate import tabulate

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

def format_as_table(data, headers=None):
    """Format dữ liệu dưới dạng bảng sử dụng tabulate"""
    if isinstance(data, str):
        try:
            # Thử parse JSON nếu data là chuỗi JSON
            parsed_data = json.loads(data)
            if isinstance(parsed_data, dict):
                # Nếu là dict, chuyển thành dạng bảng
                table_data = []
                if parsed_data and isinstance(list(parsed_data.values())[0], dict):
                    # Trường hợp dictionary lồng nhau
                    for key, value in parsed_data.items():
                        row = [key]
                        row.extend(value.values())
                        table_data.append(row)
                    headers = ["Name"] + list(list(parsed_data.values())[0].keys())
                else:
                    # Trường hợp dictionary đơn giản
                    for key, value in parsed_data.items():
                        table_data.append([key, value])
                    headers = ["Key", "Value"]
                return tabulate(table_data, headers=headers, tablefmt="pretty")
            elif isinstance(parsed_data, list):
                # Nếu là list, kiểm tra nếu là list của dictionary
                if parsed_data and isinstance(parsed_data[0], dict):
                    if not headers:
                        headers = list(parsed_data[0].keys())
                    table_data = []
                    for item in parsed_data:
                        table_data.append(list(item.values()))
                    return tabulate(table_data, headers=headers, tablefmt="pretty")
                else:
                    # List đơn giản
                    return tabulate([[item] for item in parsed_data], headers=["Value"], tablefmt="pretty")
        except:
            # Nếu không phải JSON, trả về nguyên bản
            pass
    
    if isinstance(data, list):
        # Nếu data là list
        if data and isinstance(data[0], dict):
            # List của dictionaries
            if not headers:
                headers = list(data[0].keys())
            table_data = []
            for item in data:
                table_data.append(list(item.values()))
            return tabulate(table_data, headers=headers, tablefmt="pretty")
        else:
            # List đơn giản
            return tabulate([[item] for item in data], headers=["Value"], tablefmt="pretty")
    
    # Trường hợp còn lại, trả về dạng chuỗi
    return str(data)

class DatabaseConnection:
    """
    Quản lý kết nối database với cơ chế tự động ngắt kết nối
    """
    _active_connection = None
    _active_server = None

    @classmethod
    def get_connection(cls, db_name):
        """
        Lấy kết nối đến database, tự động ngắt kết nối cũ nếu cần
        """
        # Kiểm tra nếu đã có kết nối đến database này rồi thì không cần kết nối lại
        if cls._active_connection == db_name and cls._active_server is not None:
            # Kiểm tra kết nối có còn hoạt động không
            try:
                # Thực hiện một truy vấn đơn giản để kiểm tra kết nối
                cls._active_server.execute_query("SELECT 1")
                return cls._active_server, None
            except Exception:
                # Nếu lỗi, ngắt kết nối cũ và tạo mới
                cls.close_connection()
        else:
            # Nếu đang kết nối đến database khác, ngắt kết nối đó trước
            cls.close_connection()
        
        if db_name not in available_databases:
            return None, f"Không tìm thấy database: {db_name}"
        
        db_config = available_databases[db_name]
        server = DatabaseServer()
        
        # Kết nối dựa trên loại database
        try:
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
                return None, f"Loại database không hỗ trợ: {db_config['type']}"
            
            if result.get("status") != "success":
                server.disconnect()
                return None, f"Không thể kết nối đến database: {result.get('message')}"
            
            # Lưu trữ kết nối hiện tại
            cls._active_connection = db_name
            cls._active_server = server
            
            print(f"[CONNECTION] Đã kết nối đến database: {db_name}")
            return server, None
        except Exception as e:
            try:
                server.disconnect()
            except:
                pass
            return None, f"Lỗi khi kết nối đến database: {str(e)}"
    
    @classmethod
    def close_connection(cls):
        """
        Đóng kết nối database hiện tại nếu có
        """
        if cls._active_server:
            try:
                cls._active_server.disconnect()
                print(f"[CONNECTION] Đã ngắt kết nối database: {cls._active_connection}")
            except Exception as e:
                print(f"[ERROR] Lỗi khi ngắt kết nối database: {str(e)}")
            finally:
                cls._active_server = None
                cls._active_connection = None

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
def manage_databases(action: str = "list") -> str:
    """
    Quản lý và hiển thị danh sách các database có sẵn trong hệ thống.
    
    Công cụ này cho phép quét để tìm databases mới và liệt kê tất cả các database hiện có,
    bao gồm cả SQLite và MySQL. Kết quả sẽ hiển thị dưới dạng bảng với thông tin chi tiết.
    
    Args:
        action: Hành động cần thực hiện:
                - list: Chỉ liệt kê các database hiện có (mặc định)
                - rescan: Quét lại để tìm database mới và sau đó liệt kê tất cả
    
    Returns:
        Bảng liệt kê các database có sẵn và thông tin của chúng
    """
    global available_databases
    
    # Đảm bảo đóng kết nối cũ trước khi thực hiện thao tác mới
    DatabaseConnection.close_connection()
    
    # Quét lại nếu yêu cầu
    if action.lower() == "rescan":
        old_count = len(available_databases)
        old_databases = set(available_databases.keys())
        
        available_databases = discover_databases()
        new_count = len(available_databases)
        new_databases = set(available_databases.keys())
        
        added = new_databases - old_databases
        removed = old_databases - new_databases
        
        if new_count == 0:
            return "[INFO] Không tìm thấy database nào."
        elif added:
            scan_result = f"[SUCCESS] Đã phát hiện {new_count} database. Thêm mới: {', '.join(added)}.\n\n"
        elif removed:
            scan_result = f"[INFO] Đã phát hiện {new_count} database. Đã xóa: {', '.join(removed)}.\n\n"
        else:
            scan_result = f"[INFO] Không có thay đổi, vẫn có {new_count} database có sẵn.\n\n"
    else:
        scan_result = ""
    
    # Liệt kê databases
    if not available_databases:
        return scan_result + "[INFO] Không tìm thấy database nào."
    
    table_data = []
    headers = ["Tên", "Loại", "Đường dẫn/Host", "Database", "User"]
    
    for name, config in available_databases.items():
        row = [name, config["type"]]
        
        if config["type"] == "sqlite":
            row.extend([config["path"], "", ""])
        elif config["type"] == "mysql":
            row.extend([config["host"], config["database"], config["user"]])
        
        table_data.append(row)
    
    return scan_result + format_as_table(table_data, headers)

@mcp.tool()
def explore_database(db_name: str, action: str = "list_tables", table_name: str = None, limit: int = 100, search_term: str = None) -> str:
    """
    Khám phá database và dữ liệu với nhiều tùy chọn khác nhau.
    
    Công cụ này cho phép bạn xem thông tin chi tiết về database như danh sách bảng,
    cấu trúc bảng, dữ liệu, và tìm kiếm trong dữ liệu. Tất cả kết quả đều được hiển thị
    dưới dạng bảng dễ đọc. Luôn luôn hiển thị dạng bảng
    
    Args:
        db_name: Tên database cần khám phá
        action: Hành động cần thực hiện:
                - list_tables: Liệt kê tất cả các bảng trong database
                - describe_table: Hiển thị cấu trúc của một bảng
                - get_data: Lấy dữ liệu từ một bảng
                - search_data: Tìm kiếm dữ liệu trong một bảng
        table_name: Tên bảng (bắt buộc cho describe_table, get_data, search_data)
        limit: Số lượng bản ghi tối đa trả về (cho get_data, search_data)
        search_term: Từ khóa tìm kiếm (bắt buộc cho search_data)
    
    Returns:
        Kết quả truy vấn được hiển thị dưới dạng bảng
    """
    # Kiểm tra xem database có tồn tại không
    if db_name not in available_databases:
        return f"[ERROR] Không tìm thấy database: {db_name}. Database hiện có: {', '.join(available_databases.keys())}"
    
    try:
        # Lấy kết nối đến database
        server, error = DatabaseConnection.get_connection(db_name)
        if error:
            return f"[ERROR] {error}"
        
        result = None
        
        try:
            if action == "list_tables":
                tables = server.get_table_names()
                if not tables:
                    return f"[INFO] Database {db_name} không có bảng nào."
                result = format_as_table([[table] for table in tables], ["Tên bảng"])
            
            elif action == "describe_table":
                if not table_name:
                    return "[ERROR] Vui lòng cung cấp tham số table_name."
                
                schema = server.get_table_schema(table_name)
                if not schema:
                    return f"[INFO] Không tìm thấy bảng: {table_name} trong database {db_name}."
                
                # Parse schema từ JSON string
                try:
                    schema_data = json.loads(schema)
                    result = format_as_table(schema_data)
                except:
                    result = schema  # Trả về nguyên bản nếu không parse được
            
            elif action == "get_data":
                if not table_name:
                    return "[ERROR] Vui lòng cung cấp tham số table_name."
                
                data = server.get_all_data(table_name, limit)
                if not data:
                    return f"[INFO] Không có dữ liệu trong bảng: {table_name} của database {db_name}."
                
                # Parse data từ JSON string
                try:
                    table_data = json.loads(data)
                    result = format_as_table(table_data)
                except:
                    result = data  # Trả về nguyên bản nếu không parse được
            
            elif action == "search_data":
                if not table_name:
                    return "[ERROR] Vui lòng cung cấp tham số table_name."
                if not search_term:
                    return "[ERROR] Vui lòng cung cấp tham số search_term."
                
                search_result = server.search_data(table_name, search_term, limit=limit)
                if not search_result:
                    return f"[INFO] Không tìm thấy dữ liệu phù hợp với từ khóa '{search_term}' trong bảng: {table_name}."
                
                # Parse result từ JSON string
                try:
                    search_data = json.loads(search_result)
                    result = format_as_table(search_data)
                except:
                    result = search_result  # Trả về nguyên bản nếu không parse được
            
            else:
                return f"[ERROR] Hành động không hợp lệ: {action}. Các hành động hợp lệ: list_tables, describe_table, get_data, search_data"
            
            return result
            
        except Exception as e:
            return f"[ERROR] Lỗi khi thực hiện hành động {action}: {str(e)}"
        finally:
            # Đảm bảo luôn ngắt kết nối sau khi thực hiện xong
            DatabaseConnection.close_connection()
    
    except Exception as e:
        # Đảm bảo ngắt kết nối trong trường hợp lỗi
        DatabaseConnection.close_connection()
        return f"[ERROR] {str(e)}"

@mcp.tool()
def execute_query(db_name: str, query: str) -> str:
    """
    Thực thi câu lệnh SQL chỉ đọc (SELECT) trên database và hiển thị kết quả dưới dạng bảng.
    
    Công cụ này cho phép bạn thực hiện các truy vấn SQL tùy chỉnh để lấy dữ liệu từ database.
    Chỉ các câu lệnh SELECT được cho phép để đảm bảo an toàn dữ liệu. Kết quả sẽ được hiển thị
    dưới dạng bảng dễ đọc trong terminal.
    
    Args:
        db_name: Tên database để thực thi câu lệnh
        query: Câu lệnh SQL (chỉ cho phép SELECT)
    
    Returns:
        Kết quả câu lệnh SQL được hiển thị dưới dạng bảng
    """
    # Kiểm tra câu lệnh SQL có an toàn không
    if not is_safe_query(query):
        return "[ERROR] Chỉ cho phép câu lệnh SELECT để đảm bảo chế độ chỉ đọc"
    
    try:
        # Lấy kết nối đến database
        server, error = DatabaseConnection.get_connection(db_name)
        if error:
            return f"[ERROR] {error}"
        
        try:
            result = server.execute_query(query)
            if not result:
                return f"[INFO] Không có kết quả cho truy vấn: {query}"
            
            # Parse result từ JSON string
            try:
                query_data = json.loads(result)
                return format_as_table(query_data)
            except:
                return result  # Trả về nguyên bản nếu không parse được
        finally:
            # Đảm bảo luôn ngắt kết nối sau khi thực hiện xong
            DatabaseConnection.close_connection()
    
    except Exception as e:
        # Đảm bảo ngắt kết nối trong trường hợp lỗi
        DatabaseConnection.close_connection()
        return f"[ERROR] {str(e)}"

@mcp.tool()
def get_database_summary(db_name: str) -> str:
    """
    Lấy thông tin tổng quan về database và hiển thị dưới dạng bảng.
    
    Công cụ này cung cấp tổng quan nhanh về cấu trúc và nội dung của database,
    bao gồm số lượng bảng, số lượng bản ghi, và các thông tin quan trọng khác.
    Kết quả được hiển thị dưới dạng bảng dễ đọc trong terminal.
    
    Args:
        db_name: Tên database cần lấy thông tin tổng quan
    
    Returns:
        Thông tin tổng quan về database được hiển thị dưới dạng bảng
    """
    try:
        # Lấy kết nối đến database
        server, error = DatabaseConnection.get_connection(db_name)
        if error:
            return f"[ERROR] {error}"
        
        try:
            info = server.get_database_info()
            if not info:
                return f"[INFO] Không có thông tin nào về database {db_name}."
            
            # Parse info từ JSON string
            try:
                db_info = json.loads(info)
                return format_as_table(db_info)
            except:
                return info  # Trả về nguyên bản nếu không parse được
        finally:
            # Đảm bảo luôn ngắt kết nối sau khi thực hiện xong
            DatabaseConnection.close_connection()
    
    except Exception as e:
        # Đảm bảo ngắt kết nối trong trường hợp lỗi
        DatabaseConnection.close_connection()
        return f"[ERROR] {str(e)}"

@mcp.tool()
def rescan_databases() -> str:
    """
    Quét lại tất cả các database có sẵn trong hệ thống.
    
    Công cụ này sẽ dò tìm lại tất cả các database SQLite (.db) trong thư mục hiện tại
    và kiểm tra các cấu hình MySQL trong file mysql_config.json. Hữu ích khi thêm
    database mới vào hệ thống hoặc khi cần làm mới danh sách database.
    
    Returns:
        Kết quả của quá trình quét lại database
    """
    global available_databases
    
    # Đảm bảo đóng kết nối cũ trước khi quét lại
    DatabaseConnection.close_connection()
    
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
    Thêm và kiểm tra kết nối đến MySQL database mới.
    
    Công cụ này cho phép bạn thêm cấu hình MySQL database mới vào hệ thống.
    Nó sẽ kiểm tra kết nối trước khi lưu cấu hình để đảm bảo thông tin chính xác.
    Sau khi thêm thành công, database mới sẽ có sẵn để sử dụng với các công cụ khác.
    
    Args:
        name: Tên tham chiếu đến database (sử dụng với các công cụ khác)
        host: Địa chỉ host MySQL (ví dụ: localhost, 127.0.0.1)
        user: Tên người dùng MySQL
        password: Mật khẩu MySQL
        database: Tên database MySQL cần kết nối
        port: Cổng kết nối MySQL (mặc định: 3306)
    
    Returns:
        Kết quả của việc thêm cấu hình MySQL database
    """
    global available_databases
    
    # Đảm bảo đóng tất cả kết nối trước khi thêm mới
    DatabaseConnection.close_connection()
    
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
    print(f"[INFO] Đã kiểm tra và ngắt kết nối thử nghiệm đến MySQL {host}/{database}")
    
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

if __name__ == "__main__":
    print("=== Đang quét database có sẵn... ===")
    available_databases = discover_databases()
    print(f"=== Đã phát hiện {len(available_databases)} database ===")
    
    print("=== Khởi động FastMCP Server ===")
    mcp.run(transport="stdio")