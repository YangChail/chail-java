package chail.code.sort;

import java.util.Arrays;
import java.util.Stack;

public class Sort {
	 public static void bubbleSort(int nums[]) {
	        int temp;
	        for (int i = 0; i < nums.length; i++) {//总共需要冒泡的次数
	            for (int j = 0; j < nums.length-1; j++) {//一次冒泡的过程（同时，考虑到要用到nums[j + 1]，所以为了下标不越界，j < nums.length-1）
	                if (nums[j] > nums[j + 1]) {//得到更大的数
	                    temp = nums[j];
	                    nums[j] = nums[j + 1];
	                    nums[j + 1] = temp;
	                }
	            }
	        }
	    }
	 
	 
	 
	 

	    public static void main(String[] args) {
	    	byte b[]= {67, 72, 78};
	    	System.out.println(new String(b,0,3));
	    	Stack<Integer> a=new Stack<>();
	    	a.push(1);
	    	a.peek();
	    	
//	        int a[] = new int[] { 55, 234, 567, 12, 76, 34, 21, 43, 65, 86, 54, 67 };
//	        bubbleSort(a);
//	        System.out.println(Arrays.toString(a));
	    }
}
