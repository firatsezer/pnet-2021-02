package mdp.group.ch;

import mdp.group.ch.entitys.DataCollection;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

public interface DataRepository extends CrudRepository<DataCollection, Integer>{

    @Query(value = "SELECT MIN(id) min FROM DATA_COLLECTION", nativeQuery = true)
     int findMIN();

    @Query(value = "SELECT MAX(id) max FROM DATA_COLLECTION", nativeQuery = true)
     int findMAX();
}
