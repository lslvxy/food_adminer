打包方法：打开pycharam-Terminal 输入命令

## pyinstaller 打包
带黑窗口运行版（测试时可以查看后台运行情况）:pyinstaller -D Demeter.py -p url_parse.py -p foodgrab.py -p foodpanda.py
不带黑窗口用户使用版（加一个-w参数）:pyinstaller -F  -w Demeter.py -p url_parse.py -p foodgrab.py -p foodpanda.py
mac 打包   pyinstaller -F -w -y  Demeter.py -p url_parse.py -p foodpanda.py -p foodgrab.py

## nuitka mac 打包 
python3 -m nuitka --follow-imports --standalone --macos-create-app-bundle --disable-console --enable-plugin=pyside6 --macos-app-icon=logo.png ./Adminer.py 
## nuitka win  打包 
python -m nuitka --follow-imports --standalone --onefile --disable-console --enable-plugin=pyside6 --windows-icon-from-ico=logo.png ./Demeter.py 

