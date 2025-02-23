# Semantic Model for SEC Filings

## Overview
This project aims to build a **semantic model** on top of **data parsed from SEC filings**. The primary goal is to extract meaningful financial insights from SEC Edgar Database filings for **S&P 500 securities**, structure the extracted data, and make it queryable through a **semantic model**.

## Project Components
The project consists of several key components:

1. **Downloading SEC Filings**
   - Retrieve **financial filings** from the **SEC Edgar Database** for all companies in the S&P 500.
   - Store raw filings as structured/unstructured text files.

2. **Loading Data into Snowflake**
   - Upload the downloaded filings into **Snowflake** for further processing.

3. **Parsing Filings into Chunks**
   - Implement a **parsing script** to break down financial filings into manageable text chunks.

4. **Categorizing Chunks Using a Fine-Tuned Model**
   - Use a **fine-tuned 7B parameter model** to classify each chunk based on relevant financial categories.

5. **Building a RAG (Retrieval-Augmented Generation) System**
   - Implement a **RAG search function** on top of the categorized chunks.
   - Enable efficient extraction of **financial information** from filings.

6. **Creating a Relational Data Model**
   - Use extracted financial insights to design a **relational data model** in Snowflake.
   - Organize data into **structured tables** for analytical processing.

7. **Mounting a Semantic Model**
   - Build a **semantic model** on top of the relational data model.
   - Enable **intuitive querying** and analytics on SEC financial data.

## Technology Stack
- **Snowflake** (Data storage and processing)
- **Python** (Data processing, parsing, and model execution)
- **Snowflake Connector for Python & Snowpark** (Data interaction)
- **SEC Edgar API** (Data retrieval)
- **Fine-tuned LLM (7B parameter model)** (Text classification and financial insights extraction)
- **Retrieval-Augmented Generation (RAG)** (Intelligent search)

## Project Setup
### Prerequisites
- Install Python 3.8+
- Set up a virtual environment:
  ```sh
  python -m venv venv
  source venv/bin/activate  # On macOS/Linux
  venv\Scripts\activate  # On Windows
  ```
- Install dependencies:
  ```sh
  pip install -r requirements.txt
  ```

## Running the Project
1. **Download SEC filings**
   ```sh
   python src/downloader/download_filings.py
   ```
2. **Load files into Snowflake**
   ```sh
   python src/loading/load_data.py
   ```
3. **Parse filings into chunks**
   ```sh
   python src/processing/parse_filings.py
   ```
4. **Categorize chunks using the model**
   ```sh
   python src/model/categorize_chunks.py
   ```
5. **Run the RAG search**
   ```sh
   python src/rag/run_rag.py
   ```
6. **Build the relational model in Snowflake**
   ```sh
   python src/model/build_relational_model.py
   ```
7. **Mount the semantic model**
   ```sh
   python src/model/mount_semantic_model.py
   ```

## Contributing
Contributions are welcome! Feel free to submit issues and pull requests.

## License
This project is licensed under the MIT License.

