package bcgov.aps.functions;

import bcgov.aps.JsonUtils;
import bcgov.aps.models.MetricsObject;
import bcgov.aps.models.Segments;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.api.KieServices;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;

@Slf4j
public class VerifyRules {

    transient KieSession session;

    @BeforeEach
    public void beforeAll() throws IOException {
        String rules = IOUtils
                .toString(VerifyRules.class.getResourceAsStream("../segments.drl"),
                        Charset.defaultCharset());

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
        session = kieContainer.newKieSession();

        log.info("New session");
    }

    @Test
    public void testIPRule() throws IOException {

        MetricsObject metric = new MetricsObject();
        metric.setClientIp("10.10.10.3");
        metric.setSegments(new Segments());
        session.insert(metric);
        session.fireAllRules();
        log.info("Output {}", JsonUtils.getObjectMapper().writeValueAsString(metric));
        assert JsonUtils.getObjectMapper().writeValueAsString(metric).equals("{\"segments\":{\"phase\":\"prod\",\"ip\":\"cluster_1\"},\"client_ip\":\"10.10.10.3\"}");
    }

    @Test
    public void testProdRule() throws IOException {

        MetricsObject metric = new MetricsObject();
        metric.setClientIp("10.10.10.3");
        metric.setRequestUriHost("abc.dev.local");
        metric.setSegments(new Segments());
        session.insert(metric);
        session.fireAllRules();
        log.info("Output {}", JsonUtils.getObjectMapper().writeValueAsString(metric));
        String match = "{\"segments\":{\"phase\":\"non" +
                "-prod\",\"ip\":\"cluster_1\"}," +
                "\"client_ip\":\"10.10.10.3\"," +
                "\"request_uri_host\":\"abc.dev.local\"}";

        assert JsonUtils.getObjectMapper().writeValueAsString(metric).equals(match);
    }

    @Test
    public void testProdTestRule() throws IOException {

        MetricsObject metric = new MetricsObject();
        metric.setClientIp("10.10.10.3");
        metric.setRequestUriHost("abc-test.local");
        metric.setSegments(new Segments());
        session.insert(metric);
        session.fireAllRules();
        log.info("Output {}", JsonUtils.getObjectMapper().writeValueAsString(metric));
        String match = "{\"segments\":{\"phase\":\"non" +
                "-prod\",\"ip\":\"cluster_1\"}," +
                "\"client_ip\":\"10.10.10.3\"," +
                "\"request_uri_host\":\"abc-test.local\"}";

        assert JsonUtils.getObjectMapper().writeValueAsString(metric).equals(match);
    }
}
