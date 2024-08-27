import shutil
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


class DateVariables:

    def __init__ (self):
        # fecha fichero año actual dos meses anteriores 'YYYYMM'
        self.current_date = datetime.now()
        self.month_last = self.current_date - relativedelta(months=2)
        self.twolastmnth = datetime.now().strftime('%Y') + self.month_last.strftime('%m')

        # Hallamos fechas para cambiar en etiquetas de las variables de traspasos
        self.last_year = datetime.now().year - 1

        self.previous_month = datetime.now().month - 1
        self.prev_mnth = str(self.previous_month) + 'M' + datetime.now().strftime('%y') # Ej '7M24'

        self.prev_mnth_year = str(self.previous_month) + 'M' + datetime.now().strftime('%Y') # Ej '7M2024'
        self.current_month = datetime.now().month # Ej 8
        self.crt_mnth = str(self.current_month) + 'M' + datetime.now().strftime('%y') # Ej '8M24'
        self.crt_mnth_space = ' ' + str(self.current_month) + 'M' + datetime.now().strftime('%y')

        self.previous_year = datetime.now().year - 1
        self.prev_year = '12' + 'M' + str(self.previous_year) [2:] # Ej: '12M23 MENSUAL'

        # fecha fichero año mes actual 'YYYYMM'
        self.file_date = datetime.now().strftime('%Y') + datetime.now().strftime('%m')

        # fecha fichero año actual mes anterior 'YYYYMM'
        self.current_date = datetime.now()
        self.month_last_one = self.current_date - relativedelta(months=1)
        self.file_date_lm = datetime.now().strftime('%Y') + self.month_last_one.strftime('%m')

        # fecha fichero año actual dos meses anteriores 'YYYYMM'
        self.month_last = self.current_date - relativedelta(months=2)
        self.twolastmnth = datetime.now().strftime('%Y') + self.month_last.strftime('%m')

        # Ejemplo, si hoy 2024 '12M23 MENSUAL'
        self.previous_year = datetime.now().year - 1
        self.prev_year = '12' + 'M' + str(self.previous_year) [2:] + ' MENSUAL'
        self.previous_year_dec = str(self.previous_year) + '12'

        # Ejemplo, si hoy 2024 '12M22 MENSUAL'
        self.previous_two_year = datetime.now().year - 2
        self.prev_two_year = '12' + 'M' + str(self.previous_two_year) [2:] + ' MENSUAL'
        self.previous_two_year_dec = str(self.previous_two_year) +'12'





date_vars=DateVariables()



print(date_vars.previous_two_year_dec)