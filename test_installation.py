"""
Simple script to test if all dependencies are installed correctly
"""
import sys

def test_imports():
    """Test if all required packages can be imported"""
    print("Testing package imports...")
    
    packages = [
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("datasets", "datasets"),
        ("transformers", "transformers"),
        ("jsonlines", "jsonlines"),
        ("yaml", "pyyaml"),
        ("pathlib", "pathlib"),
    ]
    
    failed = []
    success = []
    
    for name, package in packages:
        try:
            __import__(package)
            success.append(name)
            print(f"✓ {name}")
        except ImportError:
            failed.append(name)
            print(f"✗ {name} - NOT INSTALLED")
    
    print("\n" + "=" * 50)
    if failed:
        print(f"❌ {len(failed)} package(s) failed to import:")
        for name in failed:
            print(f"   - {name}")
        print("\nPlease run: pip install -r requirements.txt")
        return False
    else:
        print(f"✅ All {len(success)} packages imported successfully!")
        print("\nYou're ready to use the pipeline!")
        return True

def test_project_structure():
    """Test if project structure is correct"""
    print("\nTesting project structure...")
    
    import os
    from pathlib import Path
    
    required_files = [
        "config.yaml",
        "main.py",
        "src/pipeline/orchestrator.py",
        "src/data_ingestion/ingestor.py",
        "src/data_cleaning/cleaner.py",
        "src/data_transformation/transformer.py",
        "src/data_validation/validator.py",
    ]
    
    missing = []
    for file in required_files:
        if not Path(file).exists():
            missing.append(file)
            print(f"✗ {file} - NOT FOUND")
        else:
            print(f"✓ {file}")
    
    if missing:
        print(f"\n❌ {len(missing)} file(s) missing!")
        return False
    else:
        print(f"\n✅ All project files found!")
        return True

if __name__ == "__main__":
    print("=" * 50)
    print("Data Engineering Pipeline - Installation Test")
    print("=" * 50)
    
    imports_ok = test_imports()
    structure_ok = test_project_structure()
    
    print("\n" + "=" * 50)
    if imports_ok and structure_ok:
        print("✅ Everything looks good! You can now run:")
        print("   python main.py --input data/samples/sample_data.json")
        sys.exit(0)
    else:
        print("❌ Some issues found. Please fix them before proceeding.")
        sys.exit(1)

