package newSort;
/*
 * 位置情報をint型配列に保存してみる
 */
import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import newSort.NewSort3.SortBlock;

import quickSort.QuickSort;
import tools.MyArrayUtil;
import tools.TestTools;

public class NewSort6 <E extends Comparable> extends QuickSort<E>{
	ExecutorService executor;
	int threadsNum,arrayLength,pos,first;
	List<Callable<Object>> workers;
	int[] works;
	int worksPointer;
	BlockingQueue<SortBlock> quickSortTasks;
	boolean endArray = false;
	TestTools tt;
	long start,stop,start2,stop2,totalAdd=0,totalGet=0;


	public NewSort6() {
		threadsNum = Runtime.getRuntime().availableProcessors();
		tt = new TestTools();
	}


	@SuppressWarnings("unchecked")
	public void sort(E[] array){
		this.array = array;
		arrayLength = array.length;
		executor = Executors.newFixedThreadPool(threadsNum);
		workers = new ArrayList<Callable<Object>>(threadsNum);
		works = new int[arrayLength+100];
		worksPointer = 0;
		pos = 0;

		try {
			start = System.currentTimeMillis();
			//works[worksPointer++] = 0;
			while(!endArray){
				if(array[pos].compareTo(array[pos+1]) <= 0)
					ascendingOrder();
				else
					descendindOrder();

				pos++;  //次の数を調べる

				if(pos+1 > arrayLength-1){
					//修正
					if(pos != works[worksPointer - 1])
						works[worksPointer++] = pos;
					//works[worksPointer++] = pos;
					break;
				}
			}


			executor.invokeAll(workers);


			stop = System.currentTimeMillis();
			//マージするメソッド

			start2 = System.currentTimeMillis();
			parallelMergeSort();

			executor.invokeAll(workers);
			stop2 = System.currentTimeMillis();
		}catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
		executor.shutdown();

//		System.out.println("first : "+(stop - start));
//		System.out.println("second : "+(stop2 - start2));
//		System.out.println("totalGet : "+(int)totalGet);
//		System.out.println("totalAdd : "+(int)totalAdd);
	}

	//昇順メソッド
	@SuppressWarnings("unchecked")
	public void ascendingOrder(){
		first = pos;
		while(array[pos].compareTo(array[pos+1]) <= 0){
			if(pos+1 >= arrayLength-1){
				//works[worksPointer++] = first;
				works[worksPointer++] = pos+1;
				endArray = true;
				return;
			}
			pos++;
		}
		//works[worksPointer++] = first;
		works[worksPointer++] = pos;
		return;
	}

	//降順メソッド
	@SuppressWarnings("unchecked")
	public void descendindOrder(){
		first = pos;

		while(array[pos].compareTo(array[pos+1]) > 0){
			if(pos+1 >= arrayLength-1){
				workers.add(Executors.callable(new QuickSortWorker(first,pos+1)));
				//works[worksPointer++] = first;
				works[worksPointer++] = pos+1;
				endArray = true;
				return;
			}
			pos++;
		}

		if((pos - first) < 10){   //更新
			quickSort(first,pos);
		}else
			workers.add(Executors.callable(new QuickSortWorker(first,pos)));//修正、もっと効率よくしたい

		//works[worksPointer++] = first;
		works[worksPointer++] = pos;
		return;
	}

	//マージする部分
	public void parallelMergeSort(){
		int workSize = 0,tempSize = 0;
		int tmpPointer = -1;
		int left,mid,right,tmp;
		boolean first = false;
		//System.out.println("workSize : "+works.size());

//		for(int i = 0;i < worksPointer;i++){
//			System.out.println("works["+i+"] : "+works[i]);
//		}


		
		while(true){
				left = 0;
			
			mid = works[++tmpPointer];
			right = works[++tmpPointer];
		
			
			

			workSize = worksPointer - tempSize;  //使ったworkはもう見る必要はないためカットする
			tempSize = workSize + tempSize;   //使ったworkのサイズを保存
			
			workSize = workSize-2;
			
	
			
			
			while(workSize >= 0){
//				tt.p("left",left);
//				tt.p("mid",mid);
//				tt.p("right",right);
//				tt.p("tmpPointer",tmpPointer);
//				tt.p("worksSize",workSize);
//			
				if((right - left) < 10){
					merge(left,mid,right,new Object[right - left + 1]);
				}else
					workers.add(Executors.callable(new MergeSortWorker(left,mid,right)));

				
				works[worksPointer++] = right;
				
//				System.out.println("workSize : "+workSize);
				//一つ余りが出たら後ろに回す
				if(workSize == 1){
					works[worksPointer++] = works[++tmpPointer];
					//works[worksPointer++] = works[++tmpPointer];
					break;
				}else if(workSize == 0)
					break;
				



				//二つのworksを取り出し、left,rightを取り出す
				left = right + 1;
				mid = works[++tmpPointer];
				right = works[++tmpPointer];
				workSize = workSize-2;
				

			}



			try {
				executor.invokeAll(workers);
			} catch (InterruptedException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			
//			System.out.println("workPointer : "+worksPointer);
//			System.out.println("tempSize : "+tempSize);

			if(worksPointer - tempSize == 2){  //範囲の情報が二つのときは最後にマージして終了
				left = 0;
				mid = works[++tmpPointer];
				right = works[++tmpPointer];
				merge(left,mid,right,new Object[arrayLength]);
				break;
			}else if(worksPointer - tempSize == 1)  //範囲の情報が一つのときは完成しているので終了
				break;
		

			

		}
	}




	@SuppressWarnings("unchecked")
	public synchronized void merge(int left,int mid,int right,Object[] buff){
		int i = left,j = mid + 1,k = 0;
		//Object[] buff = buff2;

		while(i <= mid && j <= right) {
			if(array[i].compareTo(array[j]) < 0)
				buff[k++] = array[i++];
			else
				buff[k++] = array[j++];
		}

		while(i <= mid)
			buff[k++] = array[i++];
		while(j <= right)
			buff[k++] = array[j++];
		for(i = left;i <= right; i++)
			array[i] = (E) buff[i - left];
	}



	class MergeSortWorker implements Runnable{
		int left,right,mid;
		Object[] buff;
		public MergeSortWorker(int left,int right){
			this.left = left;
			this.right = right;
			mid = (left + right) / 2;
			buff = new Object[right - left + 1];
		}

		public MergeSortWorker(int left,int mid,int right){
			this.left = left;
			this.right = right;
			this.mid = mid;
			buff = new Object[right - left + 1];
		}
		public void run(){
			merge(left,mid,right,buff);
		}
	}


	class SortBlock{
		private int left,right;
		SortBlock(int left,int right){
			this.left = left;
			this.right = right;
		}

	}

	class QuickSortWorker implements Runnable {
		int left,right;
		public QuickSortWorker(int left,int right){
			this.left = left;
			this.right = right;
		}

		public void run() {
			quickSort(left,right);
		}
	}
}
