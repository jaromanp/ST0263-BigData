from mrjob.job import MRJob

class MREmployeesES(MRJob):

  def mapper(self, _, line):
    employee, economicsector, salary, year = line.split(',')
    yield (employee, (economicsector, 1))
    
  def reducer(self, employee, economicsector):
    sum_ES = 0
    for employees, cont in economicsector:
      sum_ES += cont
    yield employee, sum_ES
    
if __name__ == '__main__':
    MREmployeesES.run()
