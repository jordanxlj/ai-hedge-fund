#!/usr/bin/env python3
"""
测试运行器脚本

用法:
    python tests/run_tests.py                    # 运行所有测试
    python tests/run_tests.py test_persistent    # 运行persistent相关测试
    python tests/run_tests.py -v                 # 详细输出
"""

import sys
import subprocess
from pathlib import Path

def run_tests(test_pattern="", verbose=False):
    """运行测试"""
    # 确保在项目根目录
    project_root = Path(__file__).parent.parent
    
    # 构建pytest命令
    cmd = ["python", "-m", "pytest"]
    
    if verbose:
        cmd.append("-v")
    
    # 添加测试路径
    if test_pattern:
        cmd.append(f"tests/*{test_pattern}*")
    else:
        cmd.append("tests/")
    
    # 添加其他选项
    cmd.extend([
        "--tb=short",  # 短格式的traceback
        "--strict-markers",  # 严格的标记模式
        "--disable-warnings"  # 禁用警告
    ])
    
    print(f"运行命令: {' '.join(cmd)}")
    print(f"工作目录: {project_root}")
    
    # 运行测试
    try:
        result = subprocess.run(cmd, cwd=project_root, check=False)
        return result.returncode
    except Exception as e:
        print(f"运行测试时出错: {e}")
        return 1

def main():
    """主函数"""
    args = sys.argv[1:]
    
    verbose = "-v" in args or "--verbose" in args
    if verbose:
        args = [arg for arg in args if arg not in ["-v", "--verbose"]]
    
    test_pattern = args[0] if args else ""
    
    exit_code = run_tests(test_pattern, verbose)
    sys.exit(exit_code)

if __name__ == "__main__":
    main() 