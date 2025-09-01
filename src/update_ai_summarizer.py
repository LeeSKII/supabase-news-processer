import os
import logging
from typing import Optional
from openai import OpenAI
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv
from textwrap import dedent

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ai_summarizer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# OpenAI 配置
local_base_url = 'http://192.168.0.166:8000/v1'
model_name = 'Qwen3-235B'

client: OpenAI = OpenAI(
    api_key="EMPTY",
    base_url=local_base_url,
)

# Supabase 配置
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")

if not url or not key:
    logger.error("SUPABASE_URL 或 SUPABASE_KEY 环境变量未设置")
    exit(1)

supabase: Client = create_client(url, key)

class MetaArticle(BaseModel):
    author: Optional[str] = None
    title: Optional[str] = None
    type: Optional[str] = None
    published_date: Optional[str] = None
    tags: Optional[str] = None

def get_news_records(limit=None):
    """从数据库获取需要AI处理的新闻记录"""
    try:
        logger.info("开始获取新闻记录")
        
        # 构建查询
        query = supabase.table("News").select("id", "article", "text", "metadata")
        
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

def guided_json_completion(client: OpenAI, model: str = model_name, input_prompt: str = None):
    '''structured output json completion'''
    json_schema = MetaArticle.model_json_schema()
    system_prompt = """你是一个专业的新闻元数据提取专家。请从提供的新闻内容中提取结构化的元数据信息，包括作者、标题、类型、发布日期和标签。
    如果某些信息在文本中不存在，请返回null或空字符串。
    请确保返回的JSON格式符合提供的schema。"""
    
    completion = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": input_prompt,
            }
        ],
        extra_body={"guided_json": json_schema},
    )
    return completion.choices[0].message.reasoning_content, completion.choices[0].message.content

def ai_summarizer_completion(client: OpenAI, model: str = model_name, input_prompt: str = None):
    '''ai summarizer completion'''
    system_prompt = dedent("""你是一个专业的内容摘要专家。请为提供的内容生成一个简洁、准确的摘要。
                            摘要应该包含的主要观点、关键信息和重要细节。
                            使用中文回答。""")
    
    completion = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": input_prompt,
            }
        ],
    )
    return completion.choices[0].message.reasoning_content, completion.choices[0].message.content


def update_ai_summary(record):
    """使用AI处理单条新闻记录并更新数据库"""
    try:
        record_id = record.get("id")
        article = record.get("article")
        text = record.get("text")
        meta = record.get("metadata")
        
        if not record_id:
            logger.error("记录缺少ID字段")
            return False
        
        # 根据规则确定要处理的内容
        if article and isinstance(article, str) and article.strip():
            content_to_process = article
        elif text and isinstance(text, str) and text.strip():
            content_to_process = text
        else:
            logger.warning(f"记录ID {record_id} 既没有article也没有text字段")
            return False
        
        # 生成AI摘要
        logger.info(f"开始为记录ID {record_id} 生成AI摘要，输入数据：{content_to_process[:50]}")
        reasoning_summary, ai_summary = ai_summarizer_completion(client, model_name, content_to_process)
        logger.info(f"摘要化数据推理结果：{reasoning_summary}\n\n摘要化数据content结果：{ai_summary}")
        # 生成结构化元数据
        logger.info(f"开始为记录ID {record_id} 生成结构化元数据")
        reasoning_meta, meta_json = guided_json_completion(client, model_name, meta)
        logger.info(f"结构化数据推理结果：{reasoning_meta}\n\n结构化数据推理content结果：{meta_json}")
        # 解析JSON元数据
        try:
            meta_data = MetaArticle.model_validate_json(reasoning_meta)
        except Exception as e:
            logger.error(f"解析记录ID {record_id} 的元数据JSON时出错: {str(e)}")
        
        # 更新数据库
        update_data = {
            "summarizer":  ai_summary,
            "meta_filter": meta_data.model_dump(),
        }
        
        response = (
            supabase.table("News")
            .update(update_data)
            .eq("id", record_id)
            .execute()
        )
        
        if hasattr(response, 'error') and response.error:
            logger.error(f"更新记录ID {record_id} 时出错: {response.error}")
            return False
        
        logger.info(f"成功更新记录ID {record_id} 的AI摘要和元数据")
        return True
        
    except Exception as e:
        logger.error(f"处理记录时发生错误: {str(e)}")
        return False

def batch_update_ai_summary(limit=None):
    """批量更新新闻记录的AI摘要和元数据"""
    try:
        logger.info("开始批量更新AI摘要和元数据")
        
        # 获取需要更新的记录
        records = get_news_records(limit)
        
        if not records:
            logger.warning("没有找到需要更新的记录")
            return 0, 0
        
        processed_count = 0
        error_count = 0
        
        # 逐条更新记录
        for record in records:
            if update_ai_summary(record):
                processed_count += 1
            else:
                error_count += 1
        
        logger.info(f"批量更新完成: 成功 {processed_count} 个, 失败 {error_count} 个")
        return processed_count, error_count
        
    except Exception as e:
        logger.error(f"批量更新时发生错误: {str(e)}")
        return 0, 0

def main(limit=None):
    """主函数，协调整个AI处理流程"""
    try:
        logger.info("开始执行AI摘要和元数据处理脚本")
        
        # 执行批量更新
        processed_count, error_count = batch_update_ai_summary(limit)
        
        logger.info(f"脚本执行完成: 成功 {processed_count} 个, 失败 {error_count} 个")
        
    except KeyboardInterrupt:
        logger.info("用户中断了脚本执行")
    except Exception as e:
        logger.error(f"脚本执行时发生错误: {str(e)}")
    finally:
        logger.info("脚本执行结束")

if __name__ == "__main__":
    # 可以指定要处理的记录数量，None表示处理所有记录
    # 例如：main(limit=1) 只处理第一条记录用于测试
    main(limit=1)