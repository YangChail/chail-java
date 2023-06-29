package chail.code.test;

public class Main {
	public static int climbStairs(int n) {
        if(n==1) return 1;
        if(n==2) return 2;        
        int i=1;
        int a=1;
        int last=2;
        while(n-i>=2){
            a=a+last;
            if(i>1) {
             last=a-last;
            }
            i++;
        }
        
        return a;
    }
	public static void main(String[] args) {		
		System.out.println(climbStairs(7));
		
	}
}
