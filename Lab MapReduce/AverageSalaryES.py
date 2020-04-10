from mrjob.job import MRJob

class MRAverageSalaryES(MRJob):

  def mapper(self, _, line):
    employee, economicsector, salary, year = line.split(',')
    try:
      salary = float(salary)
    except ValueError:
      pass
    else:
      yield economicsector, salary
    
  def reducer(self, economicsector, salaries):
    sum_salaries = 0
    cont = 0
    for salary in salaries:
      sum_salaries += salary
      cont += 1
    yield economicsector, sum_salaries/cont
    
if __name__ == '__main__':
  MRAverageSalaryES.run()  