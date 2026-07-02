# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

# ① 设置源码路径
import os
import sys
sys.path.insert(0, os.path.abspath('../../src'))

project = 'sclog_lite'
copyright = '2026, Ashida.Shi'
author = 'Ashida.Shi'
release = '0.2.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# ② 启用核心扩展
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',  # 可以在文档中直接查看源码链接
    'sphinx.ext.napoleon',  # 支持更美观的 docstring 风格
    'sphinx.ext.githubpages',  # 必须添加，用于兼容 GitHub Pages
]

templates_path = ['_templates']
exclude_patterns = []

language = 'zh_CN'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']
