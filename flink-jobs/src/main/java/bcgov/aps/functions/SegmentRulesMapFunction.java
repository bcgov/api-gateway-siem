package bcgov.aps.functions;

import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.kie.api.KieServices;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.internal.io.ResourceFactory;

import java.io.File;
import java.io.StringReader;

@Slf4j
public class SegmentRulesMapFunction extends RichMapFunction<Tuple2<MetricsObject, Integer>, Tuple2<MetricsObject, Integer>> {
    private transient KieSession session;

    private String rules;

    public SegmentRulesMapFunction(String rules) {
        this.rules = rules;
    }

    @Override
    public void open(Configuration parameters) {
        KieServices kieServices = KieServices.Factory.get();
        KieRepository kieRepository =
                kieServices.getRepository();
        KieFileSystem kieFileSystem =
                kieServices.newKieFileSystem();

        kieFileSystem.write("src/main/resources/bcgov/aps/rules.drl",
                ResourceFactory.newReaderResource(new StringReader(this.rules)));

        kieServices.newKieBuilder(kieFileSystem).buildAll();

        KieContainer kieContainer =
                kieServices.newKieContainer(kieRepository.getDefaultReleaseId());
        session = kieContainer.newKieSession();
    }

    @Override
    public Tuple2<MetricsObject, Integer> map(Tuple2<MetricsObject, Integer> element) throws Exception {
        FactHandle fact = session.insert(element.f0);
        session.fireAllRules();
        session.delete(fact);
        return element;
    }
}
