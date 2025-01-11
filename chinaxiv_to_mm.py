import base64
from datetime import datetime
import hashlib
import io
from tkinter import Image
from typing import Dict, List, Tuple
from PIL import Image as PILImage
import json
from loguru import logger
import multiprocessing
import time
from pathlib import Path
import sys
import argparse
from concurrent.futures import ProcessPoolExecutor
from loguru import logger
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet

# 根据文件读取docling_parse_chinaxiv.py的输出，生成 mm parquet 文件


class ChinaXivBlock:
    def __init__(self, **kwargs) -> None:
        self.file_md5 = kwargs.get('file_md5')  # 图片md5 / json 内容 md5
        self.file_id = kwargs.get('file_id', 'default_file_id')  # articleid
        self.block_id = kwargs.get('block_id')  # 内部生成的 id
        self.text_data = kwargs.get('text_data', {})  # text 字典
        self.image_data = kwargs.get('image_data')  # 图片向量
        self.timestamp = kwargs.get('timestamp')  # 处理时间戳
        self.data_type = kwargs.get('data_type')  # 数据类型
        self.meta_data = kwargs.get('meta_data', {})  # 图片为长宽，文字参考mnbvc 文本统计信息
        # 最后一个 block 存放原始信息，个人信息是否需要过滤？
        self.raw_data = kwargs.get('raw_data', {})

    def to_dict(self) -> Dict:
        return {
            "文件md5": str(self.file_md5),
            "文件id": str(self.file_id),
            "页码": None,
            "块id": int(self.block_id),
            "文本": str(self.text_data),
            "图片": self.image_data,  # byte
            "处理时间": str(self.timestamp),
            "数据类型": str(self.data_type),
            "bounding_box": None,
            "额外信息": str(self.meta_data),
        }

    def from_dict(self, dict_data: Dict):
        self.file_md5 = dict_data.get('文件md5')
        self.file_id = dict_data.get('文件id')
        self.block_id = dict_data.get('块id')
        self.text_data = dict_data.get('文本')
        self.image_data = dict_data.get('图片')
        self.timestamp = dict_data.get('处理时间')
        self.data_type = dict_data.get('数据类型')
        self.meta_data = dict_data.get('额外信息')

    def to_json(self) -> str:
        dict_data = self.to_dict()
        dict_data["图片"] = base64.b64encode(dict_data['图片']).decode('utf-8')
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def __repr__(self) -> str:
        return rf"""
=块id: {self.block_id:04}=
文件id: {self.file_id},  块id: {self.block_id}, 处理时间: {self.timestamp}, 数据类型: {self.data_type}
文本: {self.text_data[:100]}
======
    """


def get_timestamp():
    return datetime.now().strftime("%Y%m%d")


def img_to_bytes(img_path: Path) -> Tuple[bytes, Tuple[int, int]]:
    """将图片文件转换为二进制格式

    Args:
        img_path: 图片文件路径

    Returns:
        bytes: 图片文件的二进制数据
        Tuple[int, int]: 图片的宽度和高度
    """
    try:
        with open(img_path, 'rb') as file:
            image = PILImage.open(file)
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format=image.format)
            img_byte_arr = img_byte_arr.getvalue()
        return img_byte_arr, image.size
    except Exception as e:
        logger.error(f"图片转换二进制失败: {e}")
        return None


def pdf_to_bytes(pdf_path: Path) -> bytes:
    """将PDF文件转换为二进制格式

    Args:
        pdf_path: PDF文件路径

    Returns:
        bytes: PDF文件的二进制数据
    """
    try:
        # 打开PDF文件
        with open(pdf_path, 'rb') as file:
            # 创建二进制缓冲区
            pdf_byte_arr = io.BytesIO()
            # 将PDF内容写入缓冲区
            pdf_byte_arr.write(file.read())
            # 获取二进制数据
            pdf_binary = pdf_byte_arr.getvalue()

        return pdf_binary

    except Exception as e:
        logger.error(f"PDF转换二进制失败: {e}")
        return None


def convert_to_rows(input_file: Path) -> List[ChinaXivBlock]:
    # rows 0: pdf + json + all md
    # rows 1: pages1 img + md
    # rows 2: pages2 img + md
    # ...

    # file md5 pdf file md5
    # file id file name
    # block id row index
    # text data row 0 md row 1 page md
    # image data row 0 pdf row 1 page img
    # timestamp
    # data type raw_data / page_data
    # meta data row 0 overall jsonl row 1 page size etc....

    docling_output_dir = input_file.parent / \
        f"{input_file.stem}_docling_output"
    rows = []
    # 读取 docling_output_dir 下的所有文件
    pdf_file = input_file

    pdf_file_md5 = hashlib.md5(pdf_file.read_bytes()).hexdigest()
    pdf_data = pdf_to_bytes(pdf_file)
    pdf_name = pdf_file.name
    block_id = 0

    json_file = docling_output_dir / (input_file.stem + ".json")
    json_data = json.load(json_file.open("r", encoding="utf-8"))

    md_file = docling_output_dir / (input_file.stem + ".md")
    md_data = md_file.open("r", encoding="utf-8").read()
    rows.append(ChinaXivBlock(
        file_md5=pdf_file_md5,
        file_id=pdf_name,
        block_id=block_id,
        text_data=md_data,
        image_data=pdf_data,
        timestamp=get_timestamp(),
        data_type="raw_data",
        meta_data=json.dumps(json_data, ensure_ascii=False),
    ))

    # 读取 pages 下的图文对
    pages_dir = docling_output_dir / "pages"
    img_files = sorted(list(pages_dir.glob("*.png")),
                       key=lambda x: int(x.stem.split("page-")[1]))  # 按页码排序
    md_files = sorted(list(pages_dir.glob("*.md")),
                      key=lambda x: int(x.stem.split("page-")[1]))  # 按页码排序
    assert len(img_files) == len(md_files), logger.error(
        f"The number of image files and md files is {len(img_files)}")

    # {"page_id": page_id, "page_image_size": (width, height), "page_text_length": text_length}
    for page_id, (img_file, md_file) in enumerate(zip(img_files, md_files)):
        img_data, img_size = img_to_bytes(img_file)
        md_data = md_file.open("r", encoding="utf-8").read()
        json_data = {
            "page_id": page_id,
            "page_image_size": {
                "width": img_size[0],
                "height": img_size[1],
            },
            "page_text_length": len(md_data),
        }
        block_id += 1
        rows.append(ChinaXivBlock(
            file_md5=pdf_file_md5,
            file_id=pdf_name,
            block_id=block_id,
            text_data=md_data,
            image_data=img_data,
            timestamp=get_timestamp(),
            data_type="page_data",
            meta_data=json.dumps(json_data, ensure_ascii=False),
        ))

    logger.info(
        f"process {input_file} done, {len(rows)} rows generated, {pdf_file_md5} {pdf_name}")
    return rows


def batch_to_parquet(output_file: Path, split_size: int, batchs: List[List[ChinaXivBlock]]):
    # 将 rows 写入 parquet 文件
    batch_rows = []
    # 将 batchs 按 split_size 分割，
    # 当 batch 长度大于 split_size 时，将 batch 写入 parquet 文件
    # 当 batch 长度小于 split_size 时，继续追加 batch_rows
    batch_count = 0
    split_count = 0
    for batch in batchs:
        batch_count += 1
        batch_rows.extend(batch)
        if batch_count >= split_size:
            df = pd.DataFrame([row.to_dict() for row in batch_rows])
            output_file_split = output_file.parent / \
                f"{output_file.stem}_{split_count}.parquet"
            # 使用 pyarrow 引擎
            table = pa.Table.from_pandas(df)
            # 保存为 parquet
            parquet.write_table(table, output_file_split)
            logger.info(
                f"batch {split_count} done, {output_file_split} generated")
            batch_rows = []
            batch_count = 0
            split_count += 1

    # 处理最后一个 batch
    if batch_rows:
        df = pd.DataFrame([row.to_dict() for row in batch_rows])
        output_file_last = output_file.parent / \
            f"{output_file.stem}_{split_count}.parquet"
        table = pa.Table.from_pandas(df)
        parquet.write_table(table, output_file_last)
        logger.info(f"batch {split_count} done, {output_file_last} generated")


def main():
    parser = argparse.ArgumentParser(description="Docling Convert")
    parser.add_argument("--input_file", "-i", type=Path, help="Input file")
    parser.add_argument("--output_file", "-o", type=Path, help="Output file")
    parser.add_argument("--split_size", "-s", type=int, default=200,
                        help="Split size")  # 500-1000MB 一个 parquet 文件
    parser.add_argument("--log_dir", "-l", type=Path,
                        default="logs", help="Log level")
    args = parser.parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    input_file = args.input_file
    output_file = args.output_file
    split_size = args.split_size

    log_dir = args.log_dir
    logger_file = log_dir / f"to_mm_{current_date}.log"
    logger.add(logger_file, encoding="utf-8", rotation="500MB")

    if input_file.suffix == ".txt":
        input_file_list = input_file.read_text().splitlines()
        input_file_path_list = [input_file.parent /
                                file_path for file_path in input_file_list]
        logger.info(f"input_file_path_list: {input_file_path_list}")
        batchs = [convert_to_rows(input_file)
                  for input_file in input_file_path_list]
    else:
        batchs = [convert_to_rows(input_file)]

    # 将 batch_rows 转换为 jsonl 文件
    batch_to_parquet(output_file, split_size, batchs)


if __name__ == "__main__":
    main()
