import os
import logging
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('news_update.log', encoding='utf-8'),
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
def get_news_records(limit=None):
    """从数据库获取需要更新的新闻记录"""
    try:
        logger.info("开始获取新闻记录")
        
        # 构建查询
        query = supabase.table("News").select("id", "article", "text")
        
        # 如果提供了limit参数，限制返回的记录数
        if limit:
            query = query.limit(limit)
        
        # 执行查询
        response = query.execute()
        
        if hasattr(response, 'error') and response.error:
            logger.error(f"获取新闻记录时出错: {response.error}")
            return []
        
        records = response.data
        logger.info(f"成功获取 {len(records)} 条新闻记录")
        return records
        
    except Exception as e:
        logger.error(f"获取新闻记录时发生错误: {str(e)}")
        return []

def update_describe_text(record):
    """根据规则更新单条记录的describe_text字段"""
    try:
        record_id = record.get("id")
        article = record.get("article")
        text = record.get("text")
        
        if not record_id:
            logger.error("记录缺少ID字段")
            return False
        
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
            logger.warning(f"记录ID {record_id} 既没有article也没有text字段")
        
        # 更新数据库
        response = (
            supabase.table("News")
            .update({"describe_text": describe_text})
            .eq("id", record_id)
            .execute()
        )
        
        if hasattr(response, 'error') and response.error:
            logger.error(f"更新记录ID {record_id} 时出错: {response.error}")
            return False
        
        logger.info(f"成功更新记录ID {record_id} 的describe_text字段")
        return True
        
    except Exception as e:
        logger.error(f"更新记录时发生错误: {str(e)}")
        return False

def batch_update_describe_text(limit=None):
    """批量更新新闻记录的describe_text字段"""
    try:
        logger.info("开始批量更新describe_text字段")
        
        # 获取需要更新的记录
        records = get_news_records(limit)
        
        if not records:
            logger.warning("没有找到需要更新的记录")
            return 0, 0
        
        processed_count = 0
        error_count = 0
        
        # 逐条更新记录
        for record in records:
            if update_describe_text(record):
                processed_count += 1
            else:
                error_count += 1
        
        logger.info(f"批量更新完成: 成功 {processed_count} 个, 失败 {error_count} 个")
        return processed_count, error_count
        
    except Exception as e:
        logger.error(f"批量更新时发生错误: {str(e)}")
        return 0, 0
def main(limit=None):
    """主函数，协调整个更新流程"""
    try:
        logger.info("开始执行新闻更新脚本")
        
        # 执行批量更新
        processed_count, error_count = batch_update_describe_text(limit)
        
        logger.info(f"脚本执行完成: 成功 {processed_count} 个, 失败 {error_count} 个")
        
    except KeyboardInterrupt:
        logger.info("用户中断了脚本执行")
    except Exception as e:
        logger.error(f"脚本执行时发生错误: {str(e)}")
    finally:
        logger.info("脚本执行结束")

if __name__ == "__main__":
    # 可以指定要更新的记录数量，None表示更新所有记录
    # 例如：main(limit=100) 只更新前100条记录
    main()