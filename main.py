"""
Main entry point for the Data Engineering Pipeline
"""
import argparse
import sys
from pathlib import Path
from src.pipeline.orchestrator import PipelineOrchestrator
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Data Engineering Pipeline for Generative AI Fine-tuning"
    )
    
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        required=True,
        help="Path to input data file or directory"
    )
    
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Path to output directory (default: from config)"
    )
    
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    
    args = parser.parse_args()
    
    # Check if input exists
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input path does not exist: {input_path}")
        sys.exit(1)
    
    try:
        # Initialize and run pipeline
        orchestrator = PipelineOrchestrator(config_path=args.config)
        report = orchestrator.run(input_path=str(input_path), output_path=args.output)
        
        # Print summary
        print("\n" + "=" * 80)
        print("PIPELINE SUMMARY")
        print("=" * 80)
        print(f"Total processed: {report['final_stats']['total_processed']}")
        print(f"Valid records: {report['final_stats']['total_valid']}")
        print(f"Training set: {report['final_stats']['train_size']}")
        print(f"Validation set: {report['final_stats']['val_size']}")
        print(f"\nOutput files:")
        print(f"  - {report['stages']['output']['train_file']}")
        print(f"  - {report['stages']['output']['val_file']}")
        print("=" * 80)
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

