import os
import json
import glob
import logging
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('news_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")

if not url or not key:
    logger.error("SUPABASE_URL 或 SUPABASE_KEY 环境变量未设置")
    exit(1)

supabase: Client = create_client(url, key)

def map_json_to_db(json_data):
    """将JSON数据映射到数据库表字段"""
    try:
        # 获取article和text字段
        article = json_data.get("article")
        text = json_data.get("text")
        
        # 根据规则确定describe_text的值
        if article and isinstance(article, str) and article.strip():
            # 如果article存在，取前200个字符
            describe_text = article[:200] if len(article) > 200 else article
        elif text and isinstance(text, str) and text.strip():
            # 如果article不存在，取text字段
            describe_text = text
        else:
            # 如果两者都不存在，设置为None或空字符串
            describe_text = None
        
        db_data = {
            "text": text,
            "metadata": json.dumps(json_data.get("meta", {}), ensure_ascii=False) if json_data.get("meta") else None,
            "html": json_data.get("html"),
            "images": json_data.get("images", []),
            "links": json_data.get("links", []),  # 直接获取独立的links字段
            "title": json_data.get("title"),
            "url": json_data.get("url"),
            "article": article,
            "host": json_data.get("host"),
            "word_count": json_data.get("wordCount"),
            "describe_text": describe_text
        }
        logger.debug(f"成功映射数据: {json_data.get('title', '无标题')}")
        return db_data
    except Exception as e:
        logger.error(f"映射JSON数据时出错: {str(e)}")
        raise

def process_json_files(folder_path):
    """处理指定文件夹中的所有JSON文件"""
    try:
        json_files = glob.glob(os.path.join(folder_path, "*.json"))
        processed_count = 0
        error_count = 0
        
        logger.info(f"找到 {len(json_files)} 个JSON文件")
        
        if not json_files:
            logger.warning(f"在文件夹 {folder_path} 中没有找到JSON文件")
            return 0, 0
        
        for json_file in json_files:
            try:
                logger.info(f"正在处理文件: {json_file}")
                
                # 检查文件是否存在
                if not os.path.exists(json_file):
                    logger.error(f"文件不存在: {json_file}")
                    error_count += 1
                    continue
                
                # 读取JSON文件
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON解析错误 {json_file}: {str(e)}")
                    error_count += 1
                    continue
                except Exception as e:
                    logger.error(f"读取文件 {json_file} 时出错: {str(e)}")
                    error_count += 1
                    continue
                
                # 映射数据
                try:
                    db_data = map_json_to_db(json_data)
                except Exception as e:
                    logger.error(f"映射数据 {json_file} 时出错: {str(e)}")
                    error_count += 1
                    continue
                
                # 插入数据库
                try:
                    response = (
                        supabase.table("News")
                        .insert(db_data)
                        .execute()
                    )
                    
                    if hasattr(response, 'error') and response.error:
                        logger.error(f"数据库插入错误 {json_file}: {response.error}")
                        error_count += 1
                    else:
                        logger.info(f"成功插入数据: {json_file}")
                        processed_count += 1
                        
                except Exception as e:
                    logger.error(f"数据库操作 {json_file} 时出错: {str(e)}")
                    error_count += 1
                    
            except Exception as e:
                logger.error(f"处理文件 {json_file} 时发生未知错误: {str(e)}")
                error_count += 1
        
        logger.info(f"处理完成: 成功 {processed_count} 个, 失败 {error_count} 个")
        return processed_count, error_count
        
    except Exception as e:
        logger.error(f"处理文件夹 {folder_path} 时发生错误: {str(e)}")
        return 0, 0

def main(folder_path:str):
    try:
        logger.info("开始执行新闻导入脚本")
        
        if not folder_path:
            logger.error("未提供文件夹路径")
            return
        
        if not os.path.exists(folder_path):
            logger.error(f"文件夹不存在: {folder_path}")
            return
        
        if not os.path.isdir(folder_path):
            logger.error(f"路径不是文件夹: {folder_path}")
            return
        
        logger.info(f"开始处理文件夹: {folder_path}")
        processed_count, error_count = process_json_files(folder_path)
        
        logger.info(f"脚本执行完成: 成功 {processed_count} 个, 失败 {error_count} 个")
        
    except KeyboardInterrupt:
        logger.info("用户中断了脚本执行")
    except Exception as e:
        logger.error(f"脚本执行时发生错误: {str(e)}")
    finally:
        logger.info("脚本执行结束")

if __name__ == "__main__":
    # 指定JSON文件所在的文件夹路径
    folder_path = r"C:\Users\admin\Downloads\news"
    main(folder_path)
