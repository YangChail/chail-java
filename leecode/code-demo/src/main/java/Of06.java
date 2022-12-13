import java.util.Stack;

/**
 * @author : yangc
 * @date :2022/11/15 19:16
 * @description :
 * @modyified By:
 */
public class Of06 {


    public static int[] reversePrint(ListNode<Integer> head) {
        Stack<ListNode<Integer>> A=new Stack<>();
        ListNode<Integer> a=head;
        if(head==null){
            return new int[0];
        }
        A.push(head);
        int k=0;
        while(a.next!=null){
            a=a.next;
            k++;
            A.push(a);
        }
        int[]res=new int[k];
        int i=0;
        while(!A.isEmpty()){
            res[i]=A.pop().val;
            i++;
        }
        return res;
    }


    public static void main(String[] args) {
        int[] aa=new int[]{1,3,2};
        ListNode head=new ListNode(aa[0]);
        for (int i = 1; i <aa.length; i++) {
            head.add(aa[i]);
        }
        head.print();

        int[] ints = reversePrint(head);

        for (int re : ints) {
            System.out.println(re);
        }
    }




    private static class ListNode<E> {
        E val;
        Of06.ListNode<E> next;
        ListNode( E element) {
            this.val = element;
        }


        // 添加新的结点
        public void add(int newval) {
            ListNode newNode = new ListNode(newval);
            if(this.next == null) {
                this.next = newNode;
            } else {
                this.next.add(newval);
            }
        }

        // 打印链表
        public void print() {
            System.out.print(this.val);
            if(this.next != null)
            {
                System.out.print("-->");
                this.next.print();
            }
        }
    }
}
