# Docling PDF 解析工具说明文档

## 功能概述

这是一个基于 Docling 的 PDF 文档解析工具，可以将 PDF 文件转换为 JSON 和 Markdown 格式。主要功能包括：

- PDF 文本提取
- 表格结构识别
- 页面图像生成
- 支持批量处理

当前 mian 分支主要支持 ChinaXiv 的 PDF 文件

## 安装依赖

该工具依赖于 `docling` 包及其相关组件。

## 使用方法

### 安装

```bash
pip install -r requirements.txt
```

### 执行

#### step 1 解析
将目录下的 pdf 文件转换为 json 和 markdown 文件，在原文件生成一个以原文件名_docling_output 的文件夹，里面包含 json 和overall markdown, 以及每个页面的 png 和 markdown 文件

```bash
python docling_pdf_parser.py -i <pdf_file_path> -l <log_dir>

python docling_pdf_parser.py -i data/list.txt -l logs
```

#### step 2 转换
根据解析出的文件内容，生成 parquet 文件

```bash
python chinaxiv_to_mm.py -i <pdf_file_path/pdf_file_path_list.txt> -o <output_file> -s <split_size> -l <log_dir>

python chinaxiv_to_mm.py -i data/list.txt -o data/chinaxiv.parquet -s 200 -l logs
```
在 200 设置下，每个 parquet 文件大约 5GB