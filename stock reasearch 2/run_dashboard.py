"""
Run the CSE Stock Research Dashboard

This script launches the Streamlit web dashboard.
"""
import subprocess
import sys
from pathlib import Path


def main():
    """Launch the Streamlit dashboard"""
    
    # Get the path to the app
    app_path = Path(__file__).parent / "web" / "app.py"
    
    if not app_path.exists():
        print(f"Error: Dashboard app not found at {app_path}")
        sys.exit(1)
    
    print("=" * 60)
    print("🇱🇰 CSE Stock Research Dashboard")
    print("=" * 60)
    print(f"\nStarting dashboard from: {app_path}")
    print("\nThe dashboard will open in your default web browser.")
    print("Press Ctrl+C to stop the server.\n")
    
    # Run streamlit
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run",
            str(app_path),
            "--server.headless", "false",
            "--browser.gatherUsageStats", "false",
            "--theme.primaryColor", "#667eea",
            "--theme.backgroundColor", "#FFFFFF",
            "--theme.secondaryBackgroundColor", "#f0f2f6",
            "--theme.textColor", "#262730"
        ], check=True)
    except KeyboardInterrupt:
        print("\n\nDashboard stopped.")
    except subprocess.CalledProcessError as e:
        print(f"Error running dashboard: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
