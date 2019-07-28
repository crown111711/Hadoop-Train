package com.jia.bigdata.lambda;

import io.vavr.Function2;
import io.vavr.Function4;
import io.vavr.Tuple2;

import java.math.BigInteger;
import java.util.function.Function;

import static io.vavr.API.Tuple;
import static io.vavr.API.println;


/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/27 14:56
 */
public class VaVrTest {
    public static void main(String[] args) {
        Tuple2<String, Integer> tuple2 = Tuple("Hello", 100);
        Tuple2<String, Integer> ret = tuple2.map(String::toUpperCase, Function.identity());
        println(ret._1, ret._2());

        Function<String, String> function1 = String::toUpperCase;
        Function<Object, String> function2 = function1.compose(Object::toString);

        System.out.println(function2.apply(6));

        Function4<Integer, Integer, Integer, Integer, Integer> function4 = (v1, v2, v3, v4) -> (v1 + v2) * (v3 + v4);
        Function2<Integer, Integer, Integer> func2 = function4.apply(1, 2);
        println(func2.apply(4, 5));
        println(function4.curried().apply(1).curried().apply(2).curried().apply(3).curried().apply(4));

        Function2<BigInteger, Integer, BigInteger> pow = BigInteger::pow;
        pow.memoized();
    }
}
