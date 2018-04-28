package com.jacamars.dsp.crosstalk.tests;

import java.util.List;
import java.util.ArrayList;

import com.jacamars.dsp.rtb.common.Node;
import com.jacamars.dsp.crosstalk.manager.Targeting;

/**
 * Created by ben on 7/13/17.
 */
public class TestLists {

    public static void main(String [] args) throws Exception {
        String data = "finance.yahoo.com, google.com, finance.google.com";

        List<String> list = new ArrayList();
        Targeting.getList(list, data);
        System.out.println(list);
        for (int i=0;i<list.size();i++) {
            System.out.println("'" + list.get(i) + "'");
        }

        Node n =  new Node("subdomainTest", "site.domain","MEMBER", list);
        boolean b = n.testInternal("yahoo.com");
        System.out.println("yahoo = " + b);
        b = n.testInternal("finance.yahoo.com");
        System.out.println("Finance yahoo = " + b);
        b = n.testInternal("this.notmatch.com");
        System.out.println("No match = " + b);
    }
}
