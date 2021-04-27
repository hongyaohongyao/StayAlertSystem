package xyz.hyhy.stayalert.flink.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.LoadingModelEvaluatorBuilder;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
public class PMMLUtils {
    public static void main(String[] args) throws IOException, JAXBException, SAXException {
        Evaluator evaluator = loadEvaluator("/StayAlertRFC.pmml");
        // Printing input (x1, x2, .., xn) fields
        List<? extends InputField> inputFields = evaluator.getInputFields();
//        System.out.println("Input fields: " + inputFields);
//
//        // Printing primary result (y) field(s)
//        List<? extends TargetField> targetFields = evaluator.getTargetFields();
//        System.out.println("Target field(s): " + targetFields);
//
//        // Printing secondary result (eg. probability(y), decision(y)) fields
//        List<? extends OutputField> outputFields = evaluator.getOutputFields();
//        System.out.println("Output fields: " + outputFields);
//        StayAlertPredictor predictor = new StayAlertPredictor();
//        Map<String, Object> obj = JSONObject.parseObject("{\"P1\":33.7824,\"P2\":10.2958,\"P3\":872,\"P4\":68.8073,\"P5\":0.089515,\"P6\":600,\"P7\":100,\"P8\":0,\"E1\":0,\"E2\":0,\"E3\":0,\"E4\":2,\"E5\":0.010205,\"E6\":295,\"E7\":1,\"E8\":1,\"E9\":1,\"E10\":63,\"E11\":0,\"V1\":104.28,\"V2\":0.455,\"V3\":496,\"V4\":5.99375,\"V5\":0,\"V6\":2035,\"V7\":0,\"V8\":20.8,\"V9\":0,\"V10\":4,\"V11\":12.4469,\"IsAlert\":0,\"id\":\"hoho\",\"timestamp\":1616868369059}\n");
        Map<String, Object> obj2 = JSONObject.parseObject("{\"V11\":33.7824}");
        Double d = (Double) obj2.get("V11");
        inputFields.get(3).prepare(d);
//        System.out.println(predictor.predict(obj));
    }

    /**
     * 载入PMML模型的方法
     *
     * @param pmmlFileName
     * @return
     * @throws JAXBException
     * @throws SAXException
     * @throws IOException
     */
    public static Evaluator loadEvaluator(String pmmlFileName) throws JAXBException, SAXException, IOException {
        Evaluator evaluator = new LoadingModelEvaluatorBuilder()
                .load(PMMLUtils.class.getResourceAsStream(pmmlFileName))
                .build();
        evaluator.verify(); //自校验——预热模型
        log.info("StayAlert分类评估器自校验&预热完成");
        return evaluator;
    }
}
