package bcgov.aps;

import bcgov.aps.models.MetricsObject;
import bcgov.aps.models.Segments;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.FileUtils;
import org.kie.api.KieServices;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;

@Slf4j
public class VerifyRules {
    static public void main (String[] args) throws IOException {
        String rules = FileUtils.readFileUtf8(new File("../rules.drl"));

        KieServices kieServices = KieServices.Factory.get();
        KieRepository kieRepository =
                kieServices.getRepository();
        KieFileSystem kieFileSystem =
                kieServices.newKieFileSystem();

        kieFileSystem.write("src/main/resources/bcgov/aps/rules.drl",
                ResourceFactory.newReaderResource(new StringReader(rules)));

        kieServices.newKieBuilder(kieFileSystem).buildAll();

        KieContainer kieContainer =
                kieServices.newKieContainer(kieRepository.getDefaultReleaseId());
        KieSession session = kieContainer.newKieSession();

        MetricsObject metric = new MetricsObject();
        metric.setClientIp("0.0.0.1");
        metric.setSegments(new Segments());
        session.insert(metric);
        session.fireAllRules();
        log.info("Output {}", JsonUtils.getObjectMapper().writeValueAsString(metric));

    }

}
