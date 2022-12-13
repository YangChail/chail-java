package chail.api;

import com.alibaba.druid.pool.DruidPooledConnection;
import chail.datasupport.tools.Ds2;
import chail.datasupport.tools.model.Job;
import chail.datasupport.tools.model.V2SqView;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.List;

import static chail.datasupport.tools.CheckJob.*;

@Controller
public class HelloController {

    DruidPooledConnection connection;

    @GetMapping("/job/all")
    @ResponseBody
    List<Job> allJob(HttpServletResponse response) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException, IOException {
        getConnection();
        List<Job> runningJob = getRunningJob();
        return runningJob;
    }


    @GetMapping("/job/tq")
    @ResponseBody
    List<Job> tqJob(HttpServletResponse response) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException, IOException {
        return  getTQFinish(getRunningJob());
    }

    @GetMapping("/table/tq")
    @ResponseBody
    List<Job.JobTask> tableTq(HttpServletResponse response) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException, IOException {
        return  getTQ(getRunningJob());
    }

    @GetMapping("/table/notrun")
    @ResponseBody
    List<Job.JobTask> notRunningJob(HttpServletResponse response) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException, IOException {
        return getTQDefault(getRunningJob());
    }

    @GetMapping("/table/error")
    @ResponseBody
    List<Job.JobTask> errorTab(HttpServletResponse response) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException, IOException {
        return getTQNotRunning(getRunningJob());
    }

    @GetMapping("/table/error1h")
    @ResponseBody
    List<Job.JobTask> noterror1h(HttpServletResponse response) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException, IOException {
        return tq1HFail(getRunningJob());
    }


    @GetMapping("/table/agent")
    @ResponseBody
    List<Job.JobTask> agent(HttpServletResponse response) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException, IOException {
        return engineAgent(getRunningJob());
    }


    @PostMapping("/user/login")
    @ResponseBody
    Object login(HttpServletResponse response){
        return "success";
    }

    @GetMapping("/user/info")
    @ResponseBody
    Object login1(HttpServletResponse response){
        return "success";
    }

    @PostMapping("/user/logout")
    @ResponseBody
    Object login2(HttpServletResponse response){
        return "success";
    }


//    @GetMapping("/")
//    String index(HttpServletRequest request) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException {
//        return "/index2";
//    }


    @PostMapping("/v2/config/sql")
    @ResponseBody
    Object v2Sql(HttpServletResponse response,  @RequestBody  V2SqView v2SqView){
        try {
          Ds2.updateConfig(v2SqView);
        } catch (Exception e) {
            e.printStackTrace();
            return "error:"+e.getMessage();
        }
        return "success";
    }


    @PostMapping("/v2/job/status")
    @ResponseBody
    Object runJob(HttpServletResponse response,  @RequestBody  V2SqView v2SqView){
       return null;
    }





}