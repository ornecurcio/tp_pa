# Cosas echas: 

 - DB posgres en rds publica
 - Dag con schedule por dia a las 4 am para que tome bien la fecha (local)
 - Pipeline para tomar csv local de producto y guardar top por dia en db (local)
 - Pipeline para tomar csv local de producto y guardar top ctr por dia en bd (local)
 - Api 
    endpoint /recommendations/adv/model echo, hay que mejorar la vista  

# Pendientes: 
   -Api: 
      stat/model contar los ads
      stat/varianza
      stat/

      history/{adv} cuenta de 7 dias

# Remotas
   - crear el S3 para guardar los csv y que los consuma el airflow 
   - levantar el airflow con EC2
   - dockerizar la api 