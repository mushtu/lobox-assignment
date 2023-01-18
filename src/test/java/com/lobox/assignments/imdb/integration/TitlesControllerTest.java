package com.lobox.assignments.imdb.integration;

import com.lobox.assignments.imdb.application.domain.models.Person;
import com.lobox.assignments.imdb.application.domain.models.Title;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDatabase;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbProperties;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbSerializations;
import com.lobox.assignments.imdb.integration.configuration.TestConfig;
import com.lobox.assignments.imdb.integration.support.DbUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Set;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
@Import(TestConfig.class)
@SpringBootTest()
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TitlesControllerTest {

    @Autowired
    private RocksDatabase rocks;
    @Autowired
    private RocksDbProperties properties;
    @Autowired
    private RocksDbSerializations serializations;
    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testGetTitlesWrittenDirectedByTheSameAlivePerson() throws Exception {

        // setup
        Person alivePerson1 = new Person();
        alivePerson1.setId("person1");
        alivePerson1.setBirthYear(1980);

        Person alivePerson2 = new Person();
        alivePerson2.setId("person2");
        alivePerson2.setBirthYear(1990);

        Person deadPerson = new Person();
        deadPerson.setId("person3");
        deadPerson.setBirthYear(1971);
        deadPerson.setDeathYear(2000);
        DbUtils.indexPersons(rocks, Set.of(alivePerson1, alivePerson2, deadPerson));

        Title title1 = new Title();
        title1.setId("title1");
        title1.setDirectors(Set.of(alivePerson1.getId()));
        title1.setWriters(Set.of(alivePerson1.getId()));

        Title title2 = new Title();
        title2.setId("title2");
        title2.setDirectors(Set.of(alivePerson1.getId()));
        title2.setWriters(Set.of(alivePerson2.getId()));

        Title title3 = new Title();
        title3.setId("title3");
        title3.setDirectors(Set.of(deadPerson.getId()));
        title3.setWriters(Set.of(deadPerson.getId()));

        DbUtils.indexTitles(rocks, Set.of(title1, title2, title3));


        // exercise and verify

        mockMvc.perform(
                       get("/titles/written-directed-same-alive-person")
               )
               .andDo(print())
               .andExpect(status().isOk())
               .andExpect(jsonPath("$", hasSize(1)))
               .andExpect(jsonPath("$.[*].id").value(hasItem("title1")));
    }
}
