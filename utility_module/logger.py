import logging
import sys

class OneLineExceptionFormatter(logging.Formatter):
   """Overloading the existing formatter to use our custom format"""
   def formatException(self, exc_info):
      """
      Format an exception so that it prints on a single line.
      """
      result = super().formatException(exc_info)
      return repr(result) 

   def format(self, record):
      s = super().format(record)
      # If exception is found
      if record.exc_text:
         # Convert the exception to a single line string and move  "}" to the end to preserve JSON
         s = s.replace('\n', ' ').replace("\"}"," | Traceback: ") + "\"}"
         # Convert traceback path to single quotes to maintain JSON double quites: "field":"Value" standard
         s = s.replace("File \"","File \'").replace('", line',"', line")
      return s

def get_console_handler():
   console_formatter = logging.Formatter("%(asctime)s %(filename)s: %(funcName)s: %(lineno)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(console_formatter)
   return console_handler
   
def get_file_handler(log_file_name):
   file_handler = logging.FileHandler(log_file_name)
   file_formatter = OneLineExceptionFormatter("""{"time": "%(asctime)s","file_name": "%(filename)s", "function_name": "%(funcName)s", "line_no": "%(lineno)s", "level" : "%(levelname)s", "message": "%(message)s"}""", "%Y-%m-%d %H:%M:%S")
   file_handler.setFormatter(file_formatter)
   return file_handler

def get_logger(logger_name, log_file_name='log_files/default_log.log'):
   logger = logging.getLogger(logger_name)
   logger.setLevel(logging.DEBUG) # better to have too much log than not enough
   logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler(log_file_name))

   # We want to keep logger to current file instead of root
   logger.propagate = False
   return logger    


def main():
   """Code to test logger"""
   logging = get_logger('sample','sample.log')
   logging.info('Sample message')
   try:
      x = 1 / 0
   except ZeroDivisionError as e:
      logging.exception('ZeroDivisionError: %s', e)

if __name__ == '__main__':
    main()