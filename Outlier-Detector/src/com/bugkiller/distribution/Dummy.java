package com.bugkiller.distribution;

public class Dummy {
	
	public static void main(String[] args){
		System.out.println(computeNumericSimilarityScore("60", "800"));
	}
	
	private static double computeNumericSimilarityScore(String src, String target) {
		Integer srcInt = Integer.parseInt(src);
		Integer targetInt = Integer.parseInt(target);
		Integer max = Math.max(srcInt,targetInt);
		Integer min = Math.min(srcInt, targetInt);
		int digitsInMaxValue = max.toString().length();
		System.out.println(1-(double)(Math.abs(srcInt-targetInt))/(double)min);
		return (1.0-((double)(Math.abs(srcInt-targetInt)))/(double)Math.pow(10, digitsInMaxValue));
	}

}
