package bcgov.aps;

import bcgov.aps.models.MetricsObject;
import bcgov.aps.models.Segments;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kie.api.KieServices;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;

import java.io.File;

@Slf4j
public class DroolsMetricTest {
    public static void main(String[] args) throws JsonProcessingException {
        KieServices kieServices = KieServices.Factory.get();
        KieFileSystem kieFileSystem =
                kieServices.newKieFileSystem();

        File rulePath = new File("./src/test" +
                "/resources/bcgov/aps/segments.drl");
        log.info("{}", rulePath.getAbsolutePath());

        kieFileSystem.write(ResourceFactory.newFileResource(rulePath));

        kieServices.newKieBuilder(kieFileSystem).buildAll();

        KieRepository kieRepository =
                kieServices.getRepository();
        KieContainer kieContainer =
                kieServices.newKieContainer(kieRepository.getDefaultReleaseId());

        KieSession session = kieContainer.newKieSession();

        MetricsObject metric = new MetricsObject();
        metric.setClientIp("0.0.0.1");
        metric.setSegments(new Segments());
        session.insert(metric);

        // and fire the rules
        session.fireAllRules();

        log.info(JsonUtils.getObjectMapper().writeValueAsString(metric));
    }

    @Getter
    @Setter
    static public class Person {
        private String name;
        private int age;

        // Getters and Setters
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    @Getter
    @Setter
    public static class Message {
        public static final int HELLO = 0;
        public static final int GOODBYE = 1;

        private String message;
        private int status;
    }
}
