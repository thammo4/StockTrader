import os
import sys

APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

if APP_ROOT not in sys.path:
	sys.path.append(APP_ROOT)

print('sys.path:'); print(sys.path);
