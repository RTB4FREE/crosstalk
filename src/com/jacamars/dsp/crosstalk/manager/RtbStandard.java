package com.jacamars.dsp.crosstalk.manager;

import java.util.ArrayList;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.rtb.common.Node;

/**
 * Process RTB specific rules and constraints. Turns them into RTB4FREE rules nodes.
 * @author Ben M. Faul
 *
 */
public class RtbStandard {
	/** This class's sl4j logger */
	static final Logger logger = LoggerFactory.getLogger(RtbStandard.class);
	
	public static void processStandard(ArrayNode array, List res) throws Exception {
		for (int i = 0; i < array.size(); i++) {
			ObjectNode targ = (ObjectNode) array.get(i);
			Node n = null;
			try {
				n = createRtb4FreeNode(targ);
				res.add(n);
			} catch (Exception error) {
				logger.error("Error {} processing rule:  {}", error.toString(),targ);
			}
		}
	}

	private static Node createRtb4FreeNode(ObjectNode targ) throws Exception {
		String spec = targ.get("rtbspecification").asText(null);
		spec = spec.replaceAll("request\\.", "");
		String operator = targ.get("operator").asText(null);
		String operand = targ.get("operand").asText(null);
		String operand_type = targ.get("operand_type").asText(null);
		String operand_ordinal = targ.get("operand_ordinal").asText(null);

		if (operand_type == null || operand_ordinal == null) {
			if (operator.equalsIgnoreCase("OR")) {
				if (operand_type == null)
					operand_type = "I";
				if (operand_ordinal == null)
					operand_ordinal = "L";
			} else
				throw new Exception("Missing fields operand and/or ordinal");
		}

		int rtb_required = targ.get("rtb_required").asInt(1);

		Object target;

		if (operand.startsWith("@") || operand.startsWith("$")) {
			target = operand;
		} else {
			if (operand_ordinal.equals("L")) {
				if (operand_type.equals("I")) {
					List<Integer> ilist = new ArrayList<Integer>();
					Targeting.getIntegerList(ilist, operand);
					target = ilist;
				} else {
					List<String> slist = new ArrayList<String>();
					Targeting.getList(slist, operand);
					target = slist;
				}

			} else {
				target = operand;
				if (operand_type.equalsIgnoreCase("D"))
					target = new Double(Double.parseDouble((String)target));
				else
				if (operand_type.equals("I"))
					target = new Integer(Integer.parseInt((String)target));
			}
		}

		int i = targ.hashCode();
		Node n;
		if (operator.equals("OR")) {
			List<Node> ilist = new ArrayList<Node>();
			List<String> values = new ArrayList<String>();
			Targeting.getList(values, operand);
			for (String v : values) {
				v = v.trim();
				Integer key = new Integer(v);
				JsonNode x = Scanner.gloablRtbSpecification.get(key);
				if (x != null) {
					String m_spec = x.get("rtbspecification").asText(null);
					String m_operator = x.get("operator").asText(null);
					String m_operand = x.get("operand").asText(null);
					String m_operand_ordinal = x.get("operand_ordinal").asText(null);

					Object m_target;

					if (m_operand.startsWith("@") || m_operand.startsWith("$")) {
						m_target = m_operand;
					} else {
						if (m_operand_ordinal.equals("L")) {
							List<String> list = new ArrayList<String>();
							Targeting.getList(list, m_operand);
							m_target = list;
						} else {
							m_target = operand;
						}
					}

					Node z = new Node("" + "_ZERBA_" + "-" + i, m_spec, m_operator, m_target);
					z.notPresentOk = false;
					ilist.add(z);
				}
			}

			n = new Node("ortest", null, Node.OR, ilist);
		} else {
			n = new Node("id=" + targ.get("id").asInt(), spec, operator, target);
		}

		if (rtb_required == 1)
			n.notPresentOk = false;
		else
			n.notPresentOk = true;

		return n;
	}

}
