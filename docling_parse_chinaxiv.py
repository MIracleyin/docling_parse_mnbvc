from datetime import datetime
from PIL import Image as PILImage
import json
import logging
import multiprocessing
import time
from pathlib import Path
import sys
import argparse
from concurrent.futures import ProcessPoolExecutor

from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.document import DocItem, TextItem, ListItem, SectionHeaderItem, TableItem, PictureItem
from docling_core.types.doc import ImageRefMode, PictureItem, TableItem

from loguru import logger

IMAGE_RESOLUTION_SCALE = 2.0

def get_docling_converter():
    # 后续使用 yaml / json 配置
    pipeline_options = PdfPipelineOptions()
    pipeline_options.generate_page_images = True
    pipeline_options.do_ocr = True
    pipeline_options.do_table_structure = True
    pipeline_options.table_structure_options.do_cell_matching = True
    pipeline_options.images_scale = IMAGE_RESOLUTION_SCALE
    pipeline_options.generate_page_images = True
    pipeline_options.generate_picture_images = True
    
    doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )
    
    return doc_converter

def crop_item_image(item: DocItem, page_image: PILImage.Image):
    bbox = item.prov[0].bbox.as_tuple()
    crop_image = page_image.crop(bbox)
    return crop_image

def docling_process(input_file: Path, doc_converter: DocumentConverter):

    start_time = time.time()
    conv_result = doc_converter.convert(input_file)
    end_time = time.time() - start_time
    
    logger.info(f"Docling process {input_file} finished in {end_time - start_time:.2f} seconds")
    
    output_dir = input_file.parent / f"{input_file.stem}_docling_output"
    logger.info(f"create input_file_dir {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Export Page Image and Page Text
    output_dir_pages = output_dir / "pages"
    output_dir_pages.mkdir(parents=True, exist_ok=True)
    for page_no, page in conv_result.document.pages.items():
        page_no = page.page_no
        page_image_filename = output_dir_pages / f"{input_file.stem}-page-{page_no}.png"
        with page_image_filename.open("wb") as fp:
            page.image.pil_image.save(fp, format="PNG")
        page_md = conv_result.document.export_to_markdown(page_no=page_no, image_mode=ImageRefMode.EMBEDDED)
        page_md_filename = output_dir_pages / f"{input_file.stem}-page-{page_no}.md"
        with page_md_filename.open("w", encoding="utf-8") as fp:
            fp.write(page_md)

    # Export Deep Search document JSON format:
    output_json = output_dir / f"{input_file.stem}.json"
    with (output_json).open("w", encoding="utf-8") as fp:
        fp.write(json.dumps(conv_result.document.export_to_dict(), ensure_ascii=False))

    # Export Markdown format:
    output_md = output_dir / f"{input_file.stem}.md"
    with (output_md).open("w", encoding="utf-8") as fp:
        fp.write(conv_result.document.export_to_markdown(image_mode=ImageRefMode.EMBEDDED))

        
def main():
    parser = argparse.ArgumentParser(description="Docling Convert")
    parser.add_argument("--input_file", "-i", type=Path, help="Input file")
    parser.add_argument("--log_dir", "-l", type=Path, default="logs", help="Log level")
    args = parser.parse_args()
    
    current_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    
    input_file = args.input_file
    log_dir = args.log_dir
    
    logger_file = log_dir / f"docling_convert_{current_date}.log"
    logger.add(logger_file, encoding="utf-8", rotation="500MB")
    
    # txt 文件为在数据路径下生成的 list 文件
    # ex: find . -name "*.pdf" > list.txt
    
    # 确定 pipeline_options
    doc_converter = get_docling_converter()
    
    if input_file.suffix == ".txt": 
        input_file_list = input_file.read_text().splitlines()
        input_file_path_list = [input_file.parent / file_path for file_path in input_file_list]
        logger.info(f"input_file_path_list: {input_file_path_list}")
        for file_path in input_file_path_list:
            docling_process(file_path, doc_converter)
    else:
        docling_process(input_file, doc_converter)

if __name__ == "__main__":
    main()