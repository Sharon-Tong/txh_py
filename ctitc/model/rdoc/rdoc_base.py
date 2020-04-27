#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2019-08-04
#########################################################
from docx import Document
from docxtpl import DocxTemplate
#########################################################
# 文档自动处理父类
#########################################################
class RDOCBase():
    logger = None
    doc = None
    tpl = None
    ##########################################
    # 初始化
    ##########################################
    def __init__(self, logkey="rdoc"):
        # self.logger = MyLog.getLogger(logkey, log_file='logger_rdoc.conf')
        pass
    pass
    ##########################################
    # 打开word文档
    ##########################################
    def openDoc(self, doc_name=None):
        self.doc = Document(doc_name)
    pass
    ##########################################
    # 保存word文档
    ##########################################
    def saveDoc(self, doc_name='default.docx'):
        if self.doc is None:
            self.doc = Document()
        pass
        self.doc.save(doc_name)
    pass
    ##########################################
    # 打开word模板
    ##########################################
    def openDocTemplate(self, doc_name=None):
        self.tpl = DocxTemplate(doc_name)
    pass
    ##########################################
    # 保存word模板
    ##########################################
    def saveTemplate(self, doc_name='default.docx'):
        if self.tpl is None:
            self.tpl = DocxTemplate()
        pass
        self.tpl.save(doc_name)
    pass

pass