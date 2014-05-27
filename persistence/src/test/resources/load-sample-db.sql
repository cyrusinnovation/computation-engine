DELETE FROM COMPUTATION_NODES;
   INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (1,'test','1.0','label','library'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (1,'test','1.0','name','test'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (2,'test','1.0','label','version'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (2,'test','1.0','versionNumber','1.0'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (2,'test','1.0','state','Editable'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (2,'test','1.0','lastEditDate','2014-04-07T09:30:10Z'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (3,'test','1.0','label','defaults'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (3,'test','1.0','shouldPropagateExceptions','false'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (4,'test','1.0','label','simpleComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (4,'test','1.0','package','test.computations'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (4,'test','1.0','name','MaximumTestValueComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (4,'test','1.0','changedInVersion','1.0'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (4,'test','1.0','description', 'Take the maximum of the values of the testValues map'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (4,'test','1.0','securityConfiguration','testSecurityConfiguration'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (4,'test','1.0','logger','computationLogger'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (4,'test','1.0','resultKey','maxTestValue'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (4,'test','1.0','computationExpression','val toTestImports = MutableSet()'
|| char(10) || 'val maxTuple = testValues.maxBy(aTuple => aTuple._2)'
|| char(10) || 'Some(MutableMap(maxTuple))'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (7,'test','1.0','label','imports'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (7,'test','1.0','text','scala.collection.mutable.{Map => MutableMap}'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (8,'test','1.0','label','imports'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (8,'test','1.0','text','scala.collection.mutable.{Set => MutableSet}'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (10,'test','1.0','label','inputs'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (10,'test','1.0','testValues: Map[String, Int]','testValues'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (16,'test','1.0','label','abortIfComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (16,'test','1.0','package','test.computations'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (16,'test','1.0','name','AbortIfContainsMapWithDesiredEntry'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (16,'test','1.0','changedInVersion','1.0'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (16,'test','1.0','description','See if the value is a map with one key ''b and value 5'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (16,'test','1.0','shouldPropagateExceptions','false'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (16,'test','1.0','securityConfiguration','testSecurityConfiguration'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (16,'test','1.0','logger','computationLogger'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (16,'test','1.0','predicateExpression','x == MutableMap(''b -> 5)'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (18,'test','1.0','label','innerComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (19,'test','1.0','label','ref'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (19,'test','1.0','text','test.computations.MaximumTestValueComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (20,'test','1.0','label','imports'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (20,'test','1.0','text','scala.collection.mutable.{Map => MutableMap}'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (23,'test','1.0','label','inputs'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (23,'test','1.0','x: MutableMap[Symbol, Int]','maxTestValue'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (28,'test','1.0','label','namedComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (28,'test','1.0','package','test.computations'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (28,'test','1.0','name','SequentialMaxComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (28,'test','1.0','changedInVersion','1.0'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (28,'test','1.0','description','Compute the maximum and then do a mapping computation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (29,'test','1.0','label','sequentialComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (31,'test','1.0','label','innerComputations'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (32,'test','1.0','label','ref'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (32,'test','1.0','text','test.computations.MaximumTestValueComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (33,'test','1.0','label','innerComputations'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (34,'test','1.0','label','mappingComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (34,'test','1.0','resultKey','maxTestValue'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (35,'test','1.0','label','innerComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (36,'test','1.0','label','ref'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (36,'test','1.0','text','test.computations.AbortIfContainsMapWithDesiredEntry'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (38,'test','1.0','label','inputTuple'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (38,'test','1.0','testValues: Map[String, Int]','testValues'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (42,'test','1.0','label','namedComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (42,'test','1.0','package','test.computations'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (42,'test','1.0','name','FoldingSumComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (42,'test','1.0','changedInVersion','1.0'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (42,'test','1.0','description','Sum all the values in a sequence'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (43,'test','1.0','label','foldingComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (43,'test','1.0','initialAccumulatorKey','initialAccumulator'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (46,'test','1.0','label','inputTuple'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (46,'test','1.0','testValues','addend1'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (50,'test','1.0','label','accumulatorTuple'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (50,'test','1.0','sumAccumulator','addend2'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (53,'test','1.0','label','innerComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','label','simpleComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','package','test.computations'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','name','SumComputation'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','changedInVersion','1.0'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','description','Take the sum of two addends'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','shouldPropagateExceptions','false'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','securityConfiguration','testSecurityConfiguration'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','logger','computationLogger'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','resultKey','sum'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (54,'test','1.0','computationExpression','Some(addend1 + addend2)'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (58,'test','1.0','label','inputs'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (58,'test','1.0','addend1:Int','addend1'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (61,'test','1.0','label','inputs'
); INSERT INTO COMPUTATION_NODES( ID,Library, Version,Attribute_Name, Attribute_Value ) VALUES (61,'test','1.0','addend2:Int','addend2'
);

DELETE FROM COMPUTATION_EDGES;

   INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',16,23,4
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',2,42,4
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',43,53,4
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',4,10,3
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',16,20,3
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',2,28,3
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',43,50,3
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',4,7,2
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',4,8,2
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',2,16,2
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',2,3,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',16,18,2
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',29,33,2
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',34,38,2
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',54,58,2
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',54,61,2
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',1,2,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',2,4,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',18,19,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',28,29,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',29,31,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',31,32,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',33,34,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',34,35,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',35,36,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',42,43,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',43,46,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',49,50,1
); INSERT INTO COMPUTATION_EDGES(Library,Version,Origin_Id,Target_Id,Sequence) VALUES('test','1.0',53,54,1
)