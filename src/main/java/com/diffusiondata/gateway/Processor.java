package com.diffusiondata.gateway;

import clojure.java.api.Clojure;
import clojure.lang.*;

import java.util.Map;

public class Processor {
    private final IFn loadFile;
    private final IFn entryPoint;

    private final String scriptName;

    public Processor(Map<String, String> params) {
        loadFile = Clojure.var("clojure.core", "load-file");
        entryPoint = Clojure.var(params.getOrDefault("entry", "transform"));

        scriptName = params.getOrDefault("script", "script.clj");
        loadFile.invoke(scriptName);
    }

    public void start() {
        loadFile.invoke(scriptName);
    }
    // TODO: stop, pause, resume ?

    public Object invoke(String path, String value) {
        Object newValue = entryPoint.invoke(path, value);
        return newValue;
    }
}
