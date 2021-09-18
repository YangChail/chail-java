package chail.code.sort;

import java.util.Arrays;

public class Sort2 {
	/**
	 * 冒泡排序。两次循环
	 * @param nums
	 */
	public static void bubbleSort(int nums[]) {
		for (int i = 0; i < nums.length; i++) {
			for (int j = i + 1; j < nums.length; j++) {
				if (nums[j] < nums[i]) {
					int tmp = nums[j];
					nums[j] = nums[i];
					nums[i] = tmp;
				}
			}
		}
	}
	
	/**
	 * 选择排序
	 * @param nums
	 */
	public static void selectSort(int nums[]) {
		int minIndex=0;
		for(int i=0;i<nums.length;i++) {
			minIndex=i;
			for(int j=i+1;j<nums.length-1;j++) {
				if(nums[j]<nums[minIndex]) {
					minIndex=j;
				}
			}
			int tmp=nums[minIndex];
			nums[minIndex]=nums[i];
			nums[i]=tmp;
		}
	}
	
	
	 public static boolean isPalindrome(int x) {
		 int n=x;
	        if(x<0){
	            return false;
	        }
	        int num=1;
	        while( x/10!=0){
	            num++;
	            x=x/10;
	        }
	        if(num%2==0){
	            return false;
	        } 
	        
	        for(int i=1;i<(num/2);i++){
	        	int a=n%(i*10);
	        	int b=n%((num-i)*10);
	            if(a!=b){
	                return false;
	            }            
	        }
	        return true;
	    }
	

	public static void main(String[] args) {
		System.out.println(isPalindrome(1231351));
		int a[] = new int[] { 55, 234, 567, 12, 76, 34, 21, 43, 65, 86, 54, 67 };
		selectSort(a);
		System.out.println(Arrays.toString(a));
	}
}
