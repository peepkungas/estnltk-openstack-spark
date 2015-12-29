/*package ee.estnltk_rest.controllers;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import ee.estnltk_rest.utils.Operations;

@Controller
public class MainController {

	@RequestMapping("\/\*\*\/")
	public String listPhrs(HttpServletRequest request) {
		String service=Operations.getService(request.getRequestURI());
		Operations.documentReference=
				Operations.getDocReference(request.getRequestURI(), service);
		Operations.service+=1;
		return "/"+service;
	} 	
}
*/