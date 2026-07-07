@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo ============================================================
echo   AIRMRKT 백엔드 실행
echo ============================================================
echo.

set PYCMD=
where python >nul 2>nul
if not errorlevel 1 (
    set PYCMD=python
) else (
    where py >nul 2>nul
    if not errorlevel 1 (
        set PYCMD=py
    )
)

if "%PYCMD%"=="" (
    echo [오류] Python을 찾을 수 없어요.
    echo https://www.python.org/downloads/ 에서 설치한 뒤 다시 실행해주세요.
    echo 설치 화면에서 "Add Python to PATH" 옵션을 꼭 체크하세요.
    echo.
    pause
    exit /b 1
)

echo Python 확인됨:
%PYCMD% --version
echo.

echo 필요한 패키지를 설치/확인하는 중이에요 (처음 실행 시 시간이 조금 걸려요)...
%PYCMD% -m pip install -r requirements.txt -q
if errorlevel 1 (
    echo.
    echo [오류] 패키지 설치에 실패했어요. 인터넷 연결을 확인해주세요.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo   서버를 시작할게요.
echo   고객 사이트:  http://localhost:5000
echo   관리자 화면:  http://localhost:5000/admin
echo.
echo   * 이 검은 창을 닫으면 서버가 함께 종료돼요. 그대로 두세요.
echo   * 서버를 처음 실행했다면, 아래에 관리자 초기 비밀번호가
echo     한 번만 출력돼요. 꼭 기록해두세요.
echo ============================================================
echo.

start "" cmd /c "timeout /t 2 >nul && start http://localhost:5000"
%PYCMD% app.py

echo.
echo 서버가 종료됐어요.
pause
