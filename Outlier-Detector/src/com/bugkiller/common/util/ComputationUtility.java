package com.bugkiller.common.util;

public class ComputationUtility {

	public static int findIntegerDistance(int src,int target){
		return Math.abs(src-target);
	}
	
	public static int findStringDistance(String src,String target){
        src = src.toLowerCase();
        target = target.toLowerCase();
        int[] costs = new int[target.length() + 1];
        for (int i = 0; i <= src.length(); i++) {
            int previousValue = i;
            for (int j = 0; j <= target.length(); j++) {
                if (i == 0) {
                    costs[j] = j;
                }
                else if (j > 0) {
                    int useValue = costs[j - 1];
                    if (src.charAt(i - 1) != target.charAt(j - 1)) {
                        useValue = Math.min(Math.min(useValue, previousValue), costs[j]) + 1;
                    }
                    costs[j - 1] = previousValue;
                    previousValue = useValue;

                }
            }
            if (i > 0) {
                costs[target.length()] = previousValue;
            }
        }
        return costs[target.length()];
    }
	
}
