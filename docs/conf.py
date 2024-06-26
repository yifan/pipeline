# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import sys
import os


# Insert pipeline's path into the system
sys.path.insert(0, os.path.abspath("../src"))


extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.extlinks",
    "sphinx.ext.ifconfig",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
]
source_suffix = ".rst"
master_doc = "index"
project = "Tanbih Pipeline"
year = "2020"
author = "Yifan Zhang"
copyright = "{0}, {1}".format(year, author)
release = "0.12.13"
# try:
#    from pkg_resources import get_distribution
#    version = release = get_distribution('tanbih-worker').version
# except Exception:
#    traceback.print_exc()
#    version = release = '0.12.13'

pygments_style = "trac"
templates_path = ["."]
extlinks = {
    "issue": ("https://github.com/yifan/pipeline/issues/%s", "issue %s"),
    "pr": ("https://github.com/yifan/pipeline/pulls/%s", "pullrequest %s"),
}
# import sphinx_py3doc_enhanced_theme
# html_theme = "sphinx_py3doc_enhanced_theme"
# html_theme_path = [sphinx_py3doc_enhanced_theme.get_html_theme_path()]
# html_theme_options = {
#    'githuburl': 'https://gitlab.com/meganews/pulsarworker'
# }
#
# html_use_smartypants = True
# html_last_updated_fmt = '%b %d, %Y'
# html_split_index = False
# html_sidebars = {
#   '**': ['searchbox.html', 'globaltoc.html', 'sourcelink.html'],
# }
# html_short_title = '%s-%s' % (project, version)

napoleon_use_ivar = True
napoleon_use_rtype = False
napoleon_use_param = False
