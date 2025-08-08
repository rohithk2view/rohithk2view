package com.k2view.cdbms.usercode.common.D2D;

import com.k2view.broadway.Broadway;
import com.k2view.broadway.model.Actor;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.broadway.model.Model;
import com.k2view.fabric.common.ParamConvertor;

import java.util.Optional;

public class PopulationUtil implements Actor {
    public PopulationUtil() {

    }

    @Override
    public void action(Data input, Data output, Context context) throws Exception {

        String flowName = input.string("flowName");
        try(Broadway.Flow flow = context.broadway().createFlow(flowName)){
            Model model = flow.model();
            //actor by type (if more than one return first)

            Optional<Model.Actor.Type> sourcedbquery = model.levels()
                    .flatMap(Model.Level::stages)
                    .flatMap(Model.Stage::actors)
                    .map(Model.Actor::type)
                    .filter(actor -> actor.parentType().equalsIgnoreCase("sourcedbquery"))
                    .findFirst();

            String interfaceName = "";

            if(sourcedbquery.isPresent()){
                Model.Actor.Type actor = sourcedbquery.get();
                interfaceName = actor.inputs().filter(i -> i.name().equalsIgnoreCase("interface"))
                        .findFirst()
                        .map(Model.Actor.Param::constValue)
                        .map(ParamConvertor::toString)
                        .orElse("");
            }

            output.put("result", interfaceName);

            //actor by id
            //Optional<Model.Actor> actor = model.findActor("actorName");


        }
    }
}
