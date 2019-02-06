import os
import fitz
import time
import smtplib
import json
import re
import logging
import logging.config
from logging.handlers import RotatingFileHandler
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from threading import Thread
from  queue import Queue
from email.message import EmailMessage



EMAIL_FROM = 'Телекс ОДО'
EMAIL_SUBJECT_DEFAULT = 'Телеграмма'

class FileCreatedHandler(PatternMatchingEventHandler):

    def __init__(self, path_queue, patterns=None, ignore_patterns=None,
                 ignore_directories=False, case_sensitive=False):
        super(PatternMatchingEventHandler, self).__init__()

        self._patterns = patterns
        self._ignore_patterns = ignore_patterns
        self._ignore_directories = ignore_directories
        self._case_sensitive = case_sensitive
        self.queue = path_queue

    def process(self, event):
        self.queue.put(event.src_path)

    def on_created(self, event):
        time.sleep(1)
        self.process(event)


class EmailSender(Thread):
    settings_dict = {}
    def __init__(self, text_queue, settings_dict = {}, logger =None):
        Thread.__init__(self)
        self.text_queue = text_queue
        self.settings_dict = settings_dict
        self.logger = logger

    def run(self):
        while True:
            while True:
                text = None
                if not self.text_queue.empty():
                    text = self.text_queue.get()
                if text:
                    msg, send_list = self.get_message(text)
                    recipients = self.recipients(send_list)
                    msg['To'] = ";".join(recipients)
                    self.send_mail(msg,recipients)
                    self.logger.info("Send mail with Subject:\"{}\" to: \"{}\"".format(msg['Subject'], recipients))
                if self.text_queue.empty():
                    break


    def get_message(self, text):
        send_list= None
        if text:
            msg = EmailMessage()
            msg.set_content(text)
            if self.settings_dict:
                subject = self.settings_dict['email SUBJECT default']
                text_lower = text.lower()
                if "send lists" in self.settings_dict and self.settings_dict["send lists"]:
                    send_dict = self.settings_dict["send lists"]
                    for k,v in send_dict.items():
                        if re.search(v["keyword"].lower(), text_lower):
                            subject = v["email subject"]
                            send_list = k
                msg['Subject'] = subject
                msg['From'] = self.settings_dict['email FROM']
            else:
                msg['Subject'] = EMAIL_SUBJECT_DEFAULT
                msg['From'] = EMAIL_FROM

        return msg, send_list

    def recipients(self, send_list=None):
        if send_list:
            return self.settings_dict["send lists"][send_list]['send list']
        return [self.settings_dict['email TO default'],]



    def send_mail(self, msg,recipients):
        server = smtplib.SMTP(self.settings_dict['smtp host'], self.settings_dict['smtp port'])
        if self.settings_dict['use tls']:
            server.starttls()
        server.login(self.settings_dict['smtp user'], self.settings_dict['smtp password'])
        server.sendmail(msg['From'] , recipients, msg.as_string())
        server.quit()
        time.sleep(10)



class TextParser(Thread):

    def __init__(self, path_queue, text_queue, logger= None):
        Thread.__init__(self)
        self.path_queue = path_queue
        self.text_queue = text_queue
        self.logger = logger

    def run(self):
        while True:
            while True:
                path =None
                if not self.path_queue.empty():
                    path = self.path_queue.get()
                    self.logger.info('Open file {}'.format(path))
                if path:
                    text = self.getTextFromDocument(path)
                    self.logger.info('Get te from file  \"{}\"'.format(text))
                    self.removeDoc(path)
                    self.logger.info('Remove file from disk')
                    self.text_queue.put(text)
                if self.path_queue.empty():
                    break


    def getTextFromDocument(self, path):
        if os.path.isfile(path):
            doc = fitz.open(path)
            text = ''
            for n in range(doc.pageCount):
                page = doc.loadPage(n)
                text += page.getText('text')
            doc.close()
            return text

    def removeDoc(self, path):
        if os.path.isfile(path):
            os.remove(path)



def main():

    logger = logging.getLogger("emailPrinter")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler('printer.log', mode='a', maxBytes=5*1024*1024,
                                 backupCount=2, encoding=None, delay=0)
    logger.addHandler(handler)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)


    logger.info('PrinterStarted')

    settings_dict ={}

    with open('settings.json', 'r', encoding='utf-8') as file:
        settings_dict = json.loads(file.read(), encoding='utf-8')

    path = os.path.join(settings_dict['storage path']+os.sep)
    logger.info("Path is {}".format(path))
    path_queue = Queue()
    text_queue = Queue()
    observer = Observer()
    parser = TextParser(path_queue, text_queue, logger=logger)
    sender = EmailSender(text_queue, settings_dict=settings_dict,logger=logger)
    handler = FileCreatedHandler(path_queue, patterns=settings_dict['pattern list'])
    observer.schedule(handler, path, recursive=False)
    observer.setDaemon(True)
    parser.setDaemon(True)
    sender.setDaemon(True)

    for root, dirs, files in os.walk(settings_dict['storage path']):
        for file in files:
            if '*'+os.path.splitext(file)[1] in settings_dict['pattern list']:
                logger.info('Find files to print')
                logger.info(root+os.sep+file)
                path_queue.put(root+os.sep+file)

    parser.start()
    logger.info('Start parser')
    sender.start()
    logger.info('Start sender')
    observer.start()
    logger.info('Start observer')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        parser.stop()
        sender.stop()
    observer.join()
    parser.join()
    sender.join()

if __name__ == '__main__':
    main()
