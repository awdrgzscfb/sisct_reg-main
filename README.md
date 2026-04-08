# sisct_reg

## 项目简介

基于 FastAPI + React + Vite 的注册工作台。

## 参考项目

- any-auto-register: https://github.com/zc-zhangchen/any-auto-register.git
- codex-console: https://github.com/Cong0707/codex-console.git

## 项目结构

```text
backend/   FastAPI 后端
frontend/  React + Vite 前端
main.py    后端启动入口
```

## 环境要求

- Python 3.11+
- Node.js 18+

## 安装

### 后端

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 前端

```bash
cd frontend
npm install
```

## 运行

### 方式一：前后端分开启动

后端：

```bash
source .venv/bin/activate
python main.py
```

前端：

```bash
cd frontend
npm run dev
```

访问：

```text
http://127.0.0.1:7788
```

前端开发服务器端口：

- `7788`

后端默认端口：

- `8100`

### 方式二：仅启动后端

先构建前端：

```bash
cd frontend
npm run build
```

再启动后端：

```bash
source .venv/bin/activate
python main.py
```

访问：

```text
http://127.0.0.1:8100
```

## 构建前端

```bash
cd frontend
npm run build
```
