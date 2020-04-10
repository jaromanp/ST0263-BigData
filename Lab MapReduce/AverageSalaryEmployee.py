from mrjob.job import MRJob

class MRAverageSalaryEmployee(MRJob):

  def mapper(self, _, line):
    employee, economicsector, salary, year = line.split(',')
    try:
      salary = float(salary)
    except ValueError:
      pass
    else:
      yield employee, salary
    
  def reducer(self, employee, salaries):
    sum_salaries = 0
    cont = 0
    for salary in salaries:
      sum_salaries += salary
      cont += 1
    yield employee, sum_salaries/cont
    
if __name__ == '__main__':
  MRAverageSalaryEmployee.run()  

    