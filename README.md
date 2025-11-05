# Data-engineering-pipeline-for-generative-ai-finetune
A comprehensive data engineering pipeline to prepare and process data for fine-tuning generative AI models. This pipeline handles data ingestion, cleaning, transformation, validation, and formatting into training-ready datasets.
## 🚀 Features

- **Multi-format Support**: Load data from JSON, JSONL, CSV, and TXT files
- **Data Cleaning**: Automatic text cleaning, duplicate removal, and empty field filtering
- **Format Transformation**: Convert data to instruction, conversation, or completion formats
- **Data Validation**: Quality checks and validation reports
- **Train/Val Split**: Automatic dataset splitting for training and validation
- **Configurable**: Easy-to-use YAML configuration file
- **Production Ready**: Logging, error handling, and progress tracking

## 📁 Project Structure

```
.
├── src/
│   ├── data_ingestion/      # Data loading from various formats
│   ├── data_cleaning/        # Data cleaning and preprocessing
│   ├── data_transformation/  # Format conversion for AI training
│   ├── data_validation/      # Data quality validation
│   ├── pipeline/             # Main pipeline orchestrator
│   └── utils/                # Utility functions
├── data/
│   ├── raw/                  # Raw input data
│   ├── processed/            # Processed data
│   ├── output/               # Final training datasets
│   └── samples/              # Sample data for testing
├── config.yaml               # Configuration file
├── requirements.txt          # Python dependencies
├── main.py                   # Main entry point
└── setup_instructions.md    # Detailed setup guide
```

## 🛠️ Quick Start

### 1. Install Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install packages
pip install -r requirements.txt
```

### 2. Run with Sample Data

```bash
python main.py --input data/samples/sample_data.json
```

### 3. Use Your Own Data

```bash
python main.py --input path/to/your/data.json --output data/output
```

## 📊 Supported Data Formats

### Input Formats

1. **JSON**: Array of objects
   ```json
   [
     {"instruction": "Question", "response": "Answer"},
     ...
   ]
   ```

2. **JSONL**: One JSON object per line
   ```jsonl
   {"instruction": "Question", "response": "Answer"}
   {"instruction": "Question", "response": "Answer"}
   ```

3. **CSV**: Standard CSV format
4. **TXT**: One record per line

### Output Formats

The pipeline can transform data into three formats:

1. **Instruction Format** (default): For instruction-following models
   ```json
   {
     "instruction": "...",
     "input": "...",
     "response": "...",
     "text": "Formatted text"
   }
   ```

2. **Conversation Format**: For chat models
   ```json
   {
     "messages": [
       {"role": "system", "content": "..."},
       {"role": "user", "content": "..."},
       {"role": "assistant", "content": "..."}
     ]
   }
   ```

3. **Completion Format**: For completion models
   ```json
   {
     "prompt": "...",
     "completion": "...",
     "text": "..."
   }
   ```

## ⚙️ Configuration

Edit `config.yaml` to customize:

- **Data paths**: Input/output directories
- **Output format**: instruction, conversation, or completion
- **Quality thresholds**: Min/max text length, quality scores
- **Train/Val split**: Split ratios (default: 90/10)
- **Validation settings**: Required fields, duplicate checking

## 📝 Example Usage

### Basic Usage

```bash
python main.py -i data/samples/sample_data.json
```

### Custom Output Directory

```bash
python main.py -i data/samples/sample_data.json -o my_output/
```

### Custom Configuration

```bash
python main.py -i data.json -c my_config.yaml
```

## 🔍 Pipeline Stages

1. **Data Ingestion**: Loads data from various file formats
2. **Data Cleaning**: 
   - Text cleaning and normalization
   - Duplicate removal
   - Empty field filtering
3. **Data Transformation**: Converts to AI training format
4. **Data Validation**: 
   - Field validation
   - Quality checks
   - Quality report generation
5. **Output Generation**: 
   - Train/validation split
   - JSONL file generation

## 📈 Output Files

After running the pipeline, you'll get:

- `train.jsonl`: Training dataset
- `val.jsonl`: Validation dataset

These files are ready for use with:
- Hugging Face Transformers
- OpenAI Fine-tuning
- Custom training scripts

## 🎯 Next Steps

After processing your data:

1. Review the output files in `data/output/`
2. Check the quality report in the console
3. Use the JSONL files for fine-tuning:
   ```python
   from datasets import load_dataset
   dataset = load_dataset("json", data_files="data/output/train.jsonl")
   ```

## 📚 Documentation

For detailed setup instructions, see [setup_instructions.md](setup_instructions.md)

## 🤝 Contributing

This is a template project. Feel free to customize it for your needs!

## 📄 License

This project is open source and available for educational and commercial use.
